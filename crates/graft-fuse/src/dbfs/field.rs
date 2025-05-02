use std::collections::BTreeMap;
use std::iter::repeat_n;

use rusqlite::Connection;
use rusqlite::ToSql;
use rusqlite::named_params;
use rusqlite::types::FromSql;
use rusqlite::types::FromSqlError;
use rusqlite::types::FromSqlResult;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::Value as SqlVal;
use rusqlite::types::ValueRef as SqlValRef;
use serde::Serialize;
use serde::Serializer;

use super::QUERY_FIELD;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Native = 1,
    Object = 2,
    List = 3,
}

impl ToSql for FieldKind {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(SqlVal::Integer(*self as i64)))
    }
}

impl FromSql for FieldKind {
    fn column_result(value: SqlValRef<'_>) -> FromSqlResult<Self> {
        match value {
            SqlValRef::Integer(i) if i == 1 => Ok(FieldKind::Native),
            SqlValRef::Integer(i) if i == 2 => Ok(FieldKind::Object),
            SqlValRef::Integer(i) if i == 3 => Ok(FieldKind::List),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameOrIndex {
    Name(String),
    Index(u64),
}

impl ToSql for NameOrIndex {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            NameOrIndex::Name(name) => name.to_sql(),
            NameOrIndex::Index(index) => index.to_sql(),
        }
    }
}

impl FromSql for NameOrIndex {
    fn column_result(value: SqlValRef<'_>) -> FromSqlResult<Self> {
        match value {
            SqlValRef::Text(s) => {
                let s = String::from_utf8(s.to_vec()).map_err(|_| FromSqlError::InvalidType)?;
                Ok(NameOrIndex::Name(s))
            }
            SqlValRef::Integer(i) => Ok(NameOrIndex::Index(i as u64)),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

pub fn serialize_field<S: Serializer>(
    db: &Connection,
    field_id: u64,
    serializer: S,
) -> rusqlite::Result<()> {
    let mut stmt = db.prepare_cached(QUERY_FIELD)?;
    let mut rows = stmt.query(named_params! { ":id": field_id })?;
    let mut builder = FieldBuilder::default();

    while let Some(row) = rows.next()? {
        builder.push(FieldRow {
            id: row.get(0)?,
            parent_id: row.get(1)?,
            kind: row.get(2)?,
            key: row.get(3)?,
            value: row.get(4)?,
        });
    }

    if let Err(err) = builder.finish().serialize(serializer) {
        panic!("failed to serialize field: {err}");
    }

    Ok(())
}

#[derive(Debug)]
struct FieldRow {
    id: u64,
    parent_id: u64,
    kind: FieldKind,
    key: NameOrIndex,
    value: SqlVal,
}

impl FieldRow {
    fn process(self) -> (u64, NameOrIndex, FieldVal) {
        let (id, name, container) = match self.kind {
            FieldKind::Native => (self.id, self.key, FieldVal::SqlVal(self.value)),
            FieldKind::Object => (self.id, self.key, FieldVal::new_map()),
            FieldKind::List => (self.id, self.key, FieldVal::new_list()),
        };
        (id, name, container)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FieldVal {
    SqlVal(SqlVal),
    Map(BTreeMap<String, FieldVal>),
    List(Vec<FieldVal>),
}

impl FieldVal {
    fn new_map() -> Self {
        FieldVal::Map(BTreeMap::new())
    }

    fn new_list() -> Self {
        FieldVal::List(Vec::new())
    }

    const fn null() -> Self {
        FieldVal::SqlVal(SqlVal::Null)
    }

    fn put(&mut self, key: NameOrIndex, val: FieldVal) {
        match (self, key) {
            (FieldVal::Map(map), NameOrIndex::Name(name)) => {
                map.insert(name, val);
            }
            (FieldVal::List(list), NameOrIndex::Index(index)) => {
                if index == list.len() as u64 {
                    list.push(val);
                } else if index < list.len() as u64 {
                    assert_eq!(
                        list[index as usize],
                        FieldVal::null(),
                        "duplicate index in list"
                    );
                    list[index as usize] = val;
                } else if index > list.len() as u64 {
                    list.extend(repeat_n(
                        FieldVal::null(),
                        (index - list.len() as u64 + 1) as usize,
                    ));
                    list[index as usize] = val;
                }
            }
            _ => {
                panic!("invalid field structure; expected map or list");
            }
        }
    }
}

impl Serialize for FieldVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldVal::SqlVal(val) => match val {
                SqlVal::Null => serializer.serialize_none(),
                SqlVal::Integer(i) => serializer.serialize_i64(*i),
                SqlVal::Real(f) => serializer.serialize_f64(*f),
                SqlVal::Text(s) => serializer.serialize_str(&s),
                SqlVal::Blob(b) => serializer.serialize_bytes(b),
            },
            FieldVal::Map(map) => map.serialize(serializer),
            FieldVal::List(list) => list.serialize(serializer),
        }
    }
}

#[derive(Default)]
struct FieldBuilder {
    stack: Vec<(u64, NameOrIndex, FieldVal)>,
}

impl FieldBuilder {
    fn rollup_to(&mut self, target_id: u64) {
        let mut pending = None;
        while let Some((id, _, container)) = self.stack.last_mut() {
            if let Some((_, last_key, last_val)) = pending.take() {
                container.put(last_key, last_val)
            }
            if *id == target_id {
                break;
            }
            pending = self.stack.pop();
        }
    }

    fn push(&mut self, field: FieldRow) {
        println!("pushing field: {field:?}");

        self.rollup_to(field.parent_id);

        let Some((id, _, container)) = self.stack.last_mut() else {
            return self.stack.push(field.process());
        };

        assert_eq!(
            *id, field.parent_id,
            "fields must be inserted in depth first order"
        );

        let (field_id, field_key, field_val) = field.process();
        match field_val {
            FieldVal::SqlVal(_) => {
                container.put(field_key, field_val);
            }
            _ => {
                self.stack.push((field_id, field_key, field_val));
            }
        }
    }

    fn finish(mut self) -> FieldVal {
        let Some(root_id) = self.stack.first().map(|f| f.0) else {
            return FieldVal::null();
        };
        self.rollup_to(root_id);
        let Some((id, _, root_val)) = self.stack.pop() else {
            panic!("failed to rollup stack to root; fields must be inserted in depth first order")
        };
        assert_eq!(id, root_id, "impossible: stack rolled up to non-root node");
        root_val
    }
}
