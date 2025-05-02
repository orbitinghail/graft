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

pub enum InodeFormatter {
    Json,
    Toml,
    Yaml,
}

impl InodeFormatter {
    fn from_filename(filename: &str) -> Self {
        // guess the format based on the filename
        if filename.ends_with(".json") {
            InodeFormatter::Json
        } else if filename.ends_with(".toml") {
            InodeFormatter::Toml
        } else if filename.ends_with(".yaml") || filename.ends_with(".yml") {
            InodeFormatter::Yaml
        } else {
            InodeFormatter::Json // default to JSON
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
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn parent_id(&self) -> u64 {
        self.parent_id
    }

    pub fn kind(&self) -> InodeKind {
        self.kind
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_id(&self) -> Option<u64> {
        self.field_id
    }

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

    pub fn formatter(&self) -> InodeFormatter {
        InodeFormatter::from_filename(&self.name)
    }

    pub(super) fn get_by_name(
        db: &Connection,
        parent_id: u64,
        name: &str,
    ) -> rusqlite::Result<Self> {
        let mut stmt = db.prepare_cached(
            "
                SELECT id, parent_id, kind, name, field_id
                FROM inode
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
                FROM inode
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

    pub(super) fn list_children(
        db: &Connection,
        parent_id: u64,
        limit: u64,
        offset: u64,
    ) -> rusqlite::Result<Vec<Inode>> {
        let mut stmt = db.prepare_cached(
            "
                SELECT id, parent_id, kind, name, field_id
                FROM inode
                WHERE parent_id = ? AND id != parent_id
                ORDER BY id
                LIMIT ? OFFSET ?
            ",
        )?;

        stmt.query_map([parent_id, limit, offset], |row| {
            Ok(Inode {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                kind: row.get(2)?,
                name: row.get(3)?,
                field_id: row.get(4)?,
            })
        })?
        .collect()
    }
}
