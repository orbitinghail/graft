use rusqlite::{
    Connection, ToSql, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    Dir = 1,
    File = 2,
}

impl ToSql for InodeKind {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(*self as i64)))
    }
}

impl FromSql for InodeKind {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) if i == 1 => Ok(InodeKind::Dir),
            ValueRef::Integer(i) if i == 2 => Ok(InodeKind::File),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

pub struct Inode {
    id: u64,
    parent_id: u64,
    kind: InodeKind,
    name: String,
    field_id: Option<u64>,
}

impl Inode {
    pub fn fuser_type(&self) -> fuser::FileType {
        match self.kind {
            InodeKind::File => fuser::FileType::RegularFile,
            InodeKind::Dir => fuser::FileType::Directory,
        }
    }

    pub fn unix_perm(&self) -> u16 {
        match self.kind {
            InodeKind::File => 0o644,
            InodeKind::Dir => 0o755,
        }
    }

    pub(super) fn get_by_name(
        db: &Connection,
        parent_id: u64,
        name: &str,
    ) -> rusqlite::Result<Self> {
        let mut stmt = db.prepare_cached(
            "
                SELECT id, parent_id, kind, name, field_id
                FROM inodes
                WHERE parent_id = ? AND name = ?
            ",
        )?;
        Ok(stmt.query_row(params![parent_id, name], |row| {
            Ok(Inode {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                kind: row.get(2)?,
                name: row.get(3)?,
                field_id: row.get(4)?,
            })
        })?)
    }

    pub(super) fn get_by_id(db: &Connection, id: u64) -> rusqlite::Result<Self> {
        let mut stmt = db.prepare_cached(
            "
                SELECT id, parent_id, kind, name, field_id
                FROM inodes
                WHERE id = ?
            ",
        )?;
        Ok(stmt.query_row(params![id], |row| {
            Ok(Inode {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                kind: row.get(2)?,
                name: row.get(3)?,
                field_id: row.get(4)?,
            })
        })?)
    }

    /// Selects all children of a given parent inode, starting from the given offset.
    /// Calls `cb` with each child. If `cb` returns false, end iteration early.
    pub(super) fn select_children(
        db: &Connection,
        parent_id: u64,
        offset: u64,
        mut cb: impl FnMut(Self) -> bool,
    ) -> rusqlite::Result<()> {
        let mut stmt = db.prepare_cached(
            "
                SELECT id, parent_id, kind, name, field_id
                FROM inodes
                WHERE parent_id = ? AND id != parent_id
                ORDER BY id
                LIMIT -1 OFFSET ?
            ",
        )?;
        let mut rows = stmt.query([parent_id, offset])?;
        while let Some(row) = rows.next()? {
            if !cb(Inode {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                kind: row.get(2)?,
                name: row.get(3)?,
                field_id: row.get(4)?,
            }) {
                break;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    String = 1,
    Numeric = 2,
    Boolean = 3,
    Object = 4,
    List = 5,
}

impl ToSql for FieldKind {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(*self as i64)))
    }
}

impl FromSql for FieldKind {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) if i == 1 => Ok(FieldKind::String),
            ValueRef::Integer(i) if i == 2 => Ok(FieldKind::Numeric),
            ValueRef::Integer(i) if i == 3 => Ok(FieldKind::Boolean),
            ValueRef::Integer(i) if i == 4 => Ok(FieldKind::Object),
            ValueRef::Integer(i) if i == 5 => Ok(FieldKind::List),
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
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => {
                let s = String::from_utf8(s.to_vec()).map_err(|_| FromSqlError::InvalidType)?;
                Ok(NameOrIndex::Name(s))
            }
            ValueRef::Integer(i) => Ok(NameOrIndex::Index(i as u64)),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}
