use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

use fuser::consts::FUSE_DO_READDIRPLUS;
use fuser::{
    FileAttr, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectoryPlus, ReplyEntry,
    Request,
};
use libc::ENOENT;

use crate::dbfs::inode::{Inode, InodeKind};
use crate::dbfs::{Dbfs, DbfsErr};

const TTL: Duration = Duration::from_secs(0);

struct AttrBuilder {
    uid: u32,
    gid: u32,
}

impl AttrBuilder {
    fn new(req: &Request) -> Self {
        AttrBuilder { uid: req.uid(), gid: req.gid() }
    }

    fn build(&self, inode: &Inode) -> FileAttr {
        FileAttr {
            ino: inode.id(),
            size: match inode.kind() {
                InodeKind::File => u32::MAX as u64,
                InodeKind::Dir => 0,
            },
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: inode.fuser_type(),
            perm: inode.unix_perm(),
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
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

    fn lookup(&mut self, req: &Request, parent_id: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_str().unwrap();
        match self.dbfs.get_inode_by_name(parent_id, name) {
            Ok(inode) => {
                let attr = AttrBuilder::new(req).build(&inode);
                reply.entry(&TTL, &attr, 0);
            }
            Err(DbfsErr::NotFound) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error looking up {name} in {parent_id}: {e:?}");
            }
        }
    }

    fn getattr(&mut self, req: &Request, id: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.dbfs.get_inode_by_id(id) {
            Ok(inode) => {
                let attr = AttrBuilder::new(req).build(&inode);
                reply.attr(&TTL, &attr);
            }
            Err(DbfsErr::NotFound) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error looking up inode {id}: {e}");
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
        match self.dbfs.read_inode(ino) {
            Ok(data) => {
                let start = offset as usize;
                let end = (start + size as usize).min(data.len());
                reply.data(&data[start..end]);
            }
            Err(DbfsErr::NotFound) => {
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
        start_offset: i64,
        reply: ReplyDirectoryPlus,
    ) {
        struct ReplyHelper {
            attr: AttrBuilder,
            reply: ReplyDirectoryPlus,
            offset: i64,
        }

        impl ReplyHelper {
            fn append(&mut self, name: &str, inode: &Inode) -> bool {
                let out = self.reply.add(
                    inode.id(),
                    self.offset,
                    name,
                    &TTL,
                    &self.attr.build(&inode),
                    0,
                );
                self.offset += 1;
                out
            }

            fn ok(self) {
                self.reply.ok();
            }

            fn error(self, err: libc::c_int) {
                self.reply.error(err);
            }
        }

        let mut reply = ReplyHelper {
            attr: AttrBuilder::new(req),
            reply,
            offset: start_offset,
        };

        fn handler(
            reply: &mut ReplyHelper,
            dbfs: &Dbfs,
            ino: u64,
            offset: u64,
        ) -> Result<(), DbfsErr> {
            if offset == 0 {
                let current = dbfs.get_inode_by_id(ino)?;
                reply.append(".", &current);
                reply.append("..", &dbfs.get_inode_by_id(current.parent_id())?);
            }
            for inode in dbfs.list_children(ino, 200, offset)? {
                if reply.append(inode.name(), &inode) {
                    break;
                }
            }
            Ok(())
        }

        match handler(&mut reply, &self.dbfs, ino, start_offset as u64) {
            Ok(()) => reply.ok(),
            Err(DbfsErr::NotFound) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                panic!("Error reading inode {ino}: {e:?}");
            }
        }
    }
}
