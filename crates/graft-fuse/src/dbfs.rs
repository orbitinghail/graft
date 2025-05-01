use rusqlite::{
    Connection, ToSql, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef},
};

pub const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS entries (
        -- ino is the inode number of this entry
        ino INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,

        -- parent is the parent inode of this entry; must be a directory
        parent INTEGER NOT NULL,
        kind INTEGER NOT NULL,
        name TEXT NOT NULL,

        -- contents of the file stored as JSONB
        -- must be not null for files
        contents JSONB,

        FOREIGN KEY (parent) REFERENCES entries(ino) ON DELETE CASCADE
    );
";

pub const INITIAL_DATA: &str = "
    DELETE FROM entries;
    INSERT INTO entries (ino, parent, kind, name, contents)
    VALUES
        (1, 1, 1, '/', NULL),
        (2, 1, 0, 'hello.json', '{\"hello\": \"world\"}');
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File = 0,
    Dir = 1,
}

impl EntryKind {
    pub fn fuser_type(&self) -> fuser::FileType {
        match self {
            EntryKind::File => fuser::FileType::RegularFile,
            EntryKind::Dir => fuser::FileType::Directory,
        }
    }

    pub fn unix_perm(&self) -> u16 {
        match self {
            EntryKind::File => 0o644,
            EntryKind::Dir => 0o755,
        }
    }
}

impl FromSql for EntryKind {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => Ok(match i {
                0 => EntryKind::File,
                1 => EntryKind::Dir,
                _ => return Err(FromSqlError::InvalidType),
            }),
            ValueRef::Null => Ok(EntryKind::File),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for EntryKind {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            EntryKind::File => Ok(ToSqlOutput::Owned(Value::Integer(0))),
            EntryKind::Dir => Ok(ToSqlOutput::Owned(Value::Integer(1))),
        }
    }
}

#[derive(Debug)]
pub struct DirEntry {
    pub ino: u64,
    pub kind: EntryKind,
    pub name: String,
}

#[derive(Debug)]
pub struct Dbfs {
    db: Connection,
}

impl Dbfs {
    pub fn new(db: Connection) -> Self {
        db.execute_batch(SCHEMA).expect("failed to setup db schema");
        db.execute_batch(INITIAL_DATA)
            .expect("failed to setup initial data");

        Dbfs { db }
    }

    pub fn find_entry(&self, parent: u64, name: &str) -> rusqlite::Result<DirEntry> {
        let mut stmt = self.db.prepare_cached(
            "
                SELECT ino, kind, name
                FROM entries
                WHERE parent = ? AND name = ?
            ",
        )?;
        stmt.query_row(params![parent, name], |row| {
            let ino: u64 = row.get(0)?;
            let kind: EntryKind = row.get(1)?;
            let name: String = row.get(2)?;
            Ok(DirEntry { ino, kind, name })
        })
    }

    pub fn get_entry(&self, ino: u64) -> rusqlite::Result<DirEntry> {
        let mut stmt = self.db.prepare_cached(
            "
                SELECT ino, kind, name
                FROM entries
                WHERE ino = ?
            ",
        )?;
        stmt.query_row([ino], |row| {
            let ino: u64 = row.get(0)?;
            let kind: EntryKind = row.get(1)?;
            let name: String = row.get(2)?;
            Ok(DirEntry { ino, kind, name })
        })
    }

    pub fn listdir(
        &self,
        parent: u64,
        offset: i64,
        mut cb: impl FnMut(i64, DirEntry) -> bool,
    ) -> rusqlite::Result<i64> {
        let mut stmt = self.db.prepare_cached(
            "
                SELECT ino, kind, name
                FROM entries
                WHERE parent = ? AND ino != parent
                ORDER BY ino
                LIMIT -1 OFFSET ?
            ",
        )?;

        let mut rows = stmt.query([parent, offset as u64])?;

        let mut n = offset;
        while let Some(row) = rows.next()? {
            let ino: u64 = row.get(0)?;
            let kind: EntryKind = row.get(1)?;
            let name: String = row.get(2)?;
            if !cb(n, DirEntry { ino, kind, name }) {
                break;
            }
            n += 1;
        }

        Ok(n)
    }

    pub fn read_file(&self, ino: u64) -> rusqlite::Result<serde_json::Value> {
        let mut stmt = self.db.prepare_cached(
            "
                SELECT contents
                FROM entries
                WHERE ino = ? AND kind = 0
            ",
        )?;
        stmt.query_row([ino], |row| row.get(0))
    }
}
