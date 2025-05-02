use rusqlite::{Connection, named_params};
use thiserror::Error;
use types::{FieldKind, Inode, NameOrIndex};

pub const SCHEMA: &str = include_str!("sql/schema.sql");
pub const SEED_DATA: &str = include_str!("sql/seed.sql");
pub const QUERY_FIELD: &str = include_str!("sql/query_field.sql");

pub mod types;

#[derive(Debug, Error)]
pub enum DbfsErr {
    #[error("Not found")]
    NotFound,

    #[error("Fatal database error")]
    Fatal(rusqlite::Error),
}

impl From<rusqlite::Error> for DbfsErr {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => DbfsErr::NotFound,
            _ => DbfsErr::Fatal(err),
        }
    }
}

#[derive(Debug)]
pub struct Dbfs {
    db: Connection,
}

impl Dbfs {
    pub fn new(db: Connection) -> Self {
        db.execute_batch(SCHEMA).expect("failed to setup db schema");
        db.execute_batch(SEED_DATA)
            .expect("failed to setup seed data");

        Dbfs { db }
    }

    pub fn get_inode_by_name(&self, parent_id: u64, name: &str) -> Result<Inode, DbfsErr> {
        Ok(Inode::get_by_name(&self.db, parent_id, name)?)
    }

    pub fn get_inode_by_id(&self, id: u64) -> Result<Inode, DbfsErr> {
        Ok(Inode::get_by_id(&self.db, id)?)
    }

    pub fn listdir(
        &self,
        parent_id: u64,
        offset: u64,
        cb: impl FnMut(Inode) -> bool,
    ) -> Result<(), DbfsErr> {
        Ok(Inode::select_children(&self.db, parent_id, offset, cb)?)
    }

    pub fn read_inode(&self, id: u64) -> Result<serde_json::Value, DbfsErr> {
        let mut stmt = self.db.prepare_cached(QUERY_FIELD)?;
        let mut rows = stmt.query(named_params! { ":id": id })?;
        while let Some(row) = rows.next()? {
            let id: u64 = row.get(0)?;
            let parent_id: u64 = row.get(1)?;
            let kind: FieldKind = row.get(2)?;
            let name_or_index: NameOrIndex = row.get(3)?;
            let value: rusqlite::types::Value = row.get(4)?;
        }

        todo!("build serde_json value dynamically")
    }
}
