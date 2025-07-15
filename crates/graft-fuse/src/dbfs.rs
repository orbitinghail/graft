use file_cache::FileCache;
use graft_core::byte_unit::ByteUnit;
use rusqlite::Connection;
use thiserror::Error;

use inode::{Inode, InodeFormatter};

pub const SCHEMA: &str = include_str!("sql/schema.sql");
pub const SEED_DATA: &str = include_str!("sql/seed.sql");
pub const QUERY_FIELD: &str = include_str!("sql/query_field.sql");

pub mod field;
pub mod file_cache;
pub mod inode;

const MAX_CACHE_SIZE: ByteUnit = ByteUnit::from_mb(16);
const AVG_FILE_SIZE: ByteUnit = ByteUnit::from_kb(4);
const ESTIMATED_MAX_CACHE_FILES: usize = MAX_CACHE_SIZE.as_usize() / AVG_FILE_SIZE.as_usize();

#[derive(Debug, Error)]
pub enum DbfsErr {
    #[error("Not found")]
    NotFound,

    #[error("Fatal database error: {0}")]
    Rusqlite(rusqlite::Error),

    #[error("Fatal driver error: {0}")]
    Fatal(String),
}

impl From<rusqlite::Error> for DbfsErr {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => DbfsErr::NotFound,
            _ => DbfsErr::Rusqlite(err),
        }
    }
}

#[derive(Debug)]
pub struct Dbfs {
    db: Connection,
    file_cache: FileCache,
}

impl Dbfs {
    pub fn new(db: Connection) -> Self {
        db.execute_batch(SCHEMA).expect("failed to setup db schema");
        db.execute_batch(SEED_DATA)
            .expect("failed to setup seed data");

        Dbfs {
            db,
            file_cache: FileCache::new(ESTIMATED_MAX_CACHE_FILES, MAX_CACHE_SIZE),
        }
    }

    pub fn get_inode_by_name(&self, parent_id: u64, name: &str) -> Result<Inode, DbfsErr> {
        Ok(Inode::get_by_name(&self.db, parent_id, name)?)
    }

    pub fn get_inode_by_id(&self, id: u64) -> Result<Inode, DbfsErr> {
        Ok(Inode::get_by_id(&self.db, id)?)
    }

    pub fn list_children(&self, id: u64, limit: u64, offset: u64) -> Result<Vec<Inode>, DbfsErr> {
        Ok(Inode::list_children(&self.db, id, limit, offset)?)
    }

    pub fn read_inode(&mut self, id: u64) -> Result<&Vec<u8>, DbfsErr> {
        let db = &self.db;
        self.file_cache.get_or_insert_with(id, || {
            let inode = Inode::get_by_id(db, id)?;
            let field_id = inode
                .field_id()
                .ok_or(DbfsErr::Fatal("Not a file".to_string()))?;
            let formatter = inode.formatter();
            match formatter {
                InodeFormatter::Json => {
                    let mut out = Vec::with_capacity(128);
                    let mut ser = serde_json::Serializer::pretty(&mut out);
                    field::serialize_field(&self.db, field_id, &mut ser)?;
                    out.push(b'\n');
                    Ok(out)
                }
                InodeFormatter::Toml => {
                    let mut buffer = toml::ser::Buffer::new();
                    let ser = toml::Serializer::pretty(&mut buffer);
                    field::serialize_field(&self.db, field_id, ser)?;
                    Ok(buffer.to_string().into_bytes())
                }
                InodeFormatter::Yaml => {
                    let mut out = Vec::with_capacity(128);
                    let mut ser = serde_yaml::Serializer::new(&mut out);
                    field::serialize_field(&self.db, field_id, &mut ser)?;
                    Ok(out)
                }
            }
        })
    }
}
