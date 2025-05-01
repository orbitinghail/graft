use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};
use std::u64;

use fuser::consts::FUSE_DO_READDIRPLUS;
use fuser::{
    FileAttr, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectoryPlus, ReplyEntry,
    Request,
};
use libc::ENOENT;

use crate::dbfs::{Dbfs, DirEntry, EntryKind};

const TTL: Duration = Duration::from_secs(0);

fn build_attr(req: &Request, entry: &DirEntry) -> FileAttr {
    FileAttr {
        ino: entry.ino,
        size: match entry.kind {
            EntryKind::File => u32::MAX as u64,
            EntryKind::Dir => 0,
        },
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: entry.kind.fuser_type(),
        perm: entry.kind.unix_perm(),
        nlink: 1,
        uid: req.uid(),
        gid: req.gid(),
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

#[derive(Debug)]
pub struct FuseFs {
    dbfs: Dbfs,
}

impl FuseFs {
    pub fn new(dbfs: Dbfs) -> Self {
        FuseFs { dbfs }
    }
}

impl Filesystem for FuseFs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        config
            .add_capabilities(FUSE_DO_READDIRPLUS)
            .expect("failed to add capabilities");
        Ok(())
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_str().unwrap();
        match self.dbfs.find_entry(parent, name) {
            Ok(entry) => {
                let attr = build_attr(req, &entry);
                reply.entry(&TTL, &attr, 0);
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error looking up {name} in {parent}: {e}");
            }
        }
    }

    fn getattr(&mut self, req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.dbfs.get_entry(ino) {
            Ok(entry) => {
                let attr = build_attr(req, &entry);
                reply.attr(&TTL, &attr);
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error looking up inode {ino}: {e}");
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        match self.dbfs.read_file(ino) {
            Ok(data) => {
                let mut contents = serde_json::to_vec_pretty(&data).unwrap();
                contents.push('\n' as u8);
                let start = offset as usize;
                let end = (start + size as usize).min(contents.len());
                reply.data(&contents[start..end]);
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error reading inode {ino}: {e:?}");
            }
        }
    }

    fn readdirplus(
        &mut self,
        req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let mut extra = 0;
        if offset == 0 {
            extra = 2;
            reply.add(
                1,
                0,
                OsStr::new("."),
                &TTL,
                &build_attr(
                    req,
                    &DirEntry {
                        ino: 1,
                        kind: EntryKind::Dir,
                        name: ".".to_string(),
                    },
                ),
                0,
            );
            reply.add(
                1,
                1,
                OsStr::new(".."),
                &TTL,
                &build_attr(
                    req,
                    &DirEntry {
                        ino: 1,
                        kind: EntryKind::Dir,
                        name: ".".to_string(),
                    },
                ),
                0,
            );
        }

        let n = self
            .dbfs
            .listdir(ino, offset, |off, entry| {
                !reply.add(
                    entry.ino,
                    off + extra,
                    entry.name.as_str(),
                    &TTL,
                    &build_attr(req, &entry),
                    0,
                )
            })
            .expect("failed to listdir");

        if n == 0 {
            reply.error(ENOENT);
        } else {
            reply.ok();
        }
    }
}
