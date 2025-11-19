use std::{fmt::Write, fs::File, io::Read, path::PathBuf};

use bytes::{Bytes, BytesMut};
use culprit::{Culprit, ResultExt};
use graft_core::{
    PageCount, PageIdx, VolumeId,
    lsn::LSNRangeExt,
    page::{PAGESIZE, Page},
};
use graft_kernel::{
    graft::AheadStatus, rt::runtime::Runtime, tag_handle::TagHandle, volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};
use indoc::{formatdoc, indoc, writedoc};
use sqlite_plugin::{
    vars::SQLITE_ERROR,
    vfs::{Pragma, PragmaErr},
};
use tryiter::TryIteratorExt;
use zerocopy::FromBytes;

use crate::{dbg::SqliteHeader, file::vol_file::VolFile, vfs::ErrCtx};

pub enum GraftPragma {
    /// `pragma graft_list;`
    List,

    /// `pragma graft_tags;`
    Tags,

    /// `pragma graft_switch = "local_vid[:remote]";`
    Switch {
        graft: VolumeId,
        remote: Option<VolumeId>,
    },

    /// `pragma graft_clone [= "remote_vid"];`
    Clone { remote: Option<VolumeId> },

    /// `pragma graft_fork;`
    Fork,

    /// `pragma graft_info;`
    Info,

    /// `pragma graft_status;`
    Status,

    /// `pragma graft_snapshot;`
    Snapshot,

    /// `pragma graft_fetch;`
    Fetch,

    /// `pragma graft_pull;`
    Pull,

    /// `pragma graft_push;`
    Push,

    /// `pragma graft_audit;`
    Audit,

    /// `pragma graft_hydrate;`
    Hydrate,

    /// `pragma graft_version;`
    Version,

    /// `pragma graft_import = "PATH";`
    Import(PathBuf),

    /// `pragma graft_dump_header;`
    DumpSqliteHeader,
}

impl TryFrom<&Pragma<'_>> for GraftPragma {
    type Error = PragmaErr;

    fn try_from(p: &Pragma<'_>) -> Result<Self, Self::Error> {
        if let Some((prefix, suffix)) = p.name.split_once("_")
            && prefix == "graft"
        {
            return match suffix {
                "list" => Ok(GraftPragma::List),
                "tags" => Ok(GraftPragma::Tags),
                "clone" => {
                    if let Some(arg) = p.arg {
                        let remote = arg
                            .parse::<VolumeId>()
                            .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))?;
                        Ok(GraftPragma::Clone { remote: Some(remote) })
                    } else {
                        Ok(GraftPragma::Clone { remote: None })
                    }
                }
                "fork" => Ok(GraftPragma::Fork),
                "new" => Ok(GraftPragma::Switch { graft: VolumeId::random(), remote: None }),
                "switch" => {
                    let arg = p.arg.ok_or_else(|| PragmaErr::required_arg(p))?;
                    let (prefix, suffix) = arg.split_once(":").unwrap_or((arg, ""));
                    let graft = prefix
                        .parse::<VolumeId>()
                        .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))?;
                    let remote =
                        if !suffix.is_empty() {
                            Some(suffix.parse::<VolumeId>().map_err(|err| {
                                PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string()))
                            })?)
                        } else {
                            None
                        };
                    Ok(GraftPragma::Switch { graft, remote })
                }
                "info" => Ok(GraftPragma::Info),
                "status" => Ok(GraftPragma::Status),
                "snapshot" => Ok(GraftPragma::Snapshot),
                "fetch" => Ok(GraftPragma::Fetch),
                "pull" => Ok(GraftPragma::Pull),
                "push" => Ok(GraftPragma::Push),
                "audit" => Ok(GraftPragma::Audit),
                "hydrate" => Ok(GraftPragma::Hydrate),
                "version" => Ok(GraftPragma::Version),
                "import" => {
                    let arg = p.arg.ok_or_else(|| PragmaErr::required_arg(p))?;
                    let path = PathBuf::from(arg);
                    Ok(GraftPragma::Import(path))
                }
                "dump_header" => Ok(GraftPragma::DumpSqliteHeader),
                _ => Err(PragmaErr::Fail(
                    SQLITE_ERROR,
                    Some(format!("invalid graft pragma `{}`", p.name)),
                )),
            };
        }
        Err(PragmaErr::NotFound)
    }
}

impl GraftPragma {
    pub fn eval(
        self,
        runtime: &Runtime,
        file: &mut VolFile,
    ) -> Result<Option<String>, Culprit<ErrCtx>> {
        match self {
            GraftPragma::List => Ok(Some(format_grafts(runtime, file.handle().graft())?)),
            GraftPragma::Tags => Ok(Some(format_tags(runtime)?)),

            GraftPragma::Clone { remote } => {
                file.handle_mut().clone_remote(remote).or_into_ctx()?;
                let remote = file.handle().remote().or_into_ctx()?;
                Ok(Some(format!(
                    "Created new Graft {} with remote Volume {}",
                    file.handle().graft(),
                    remote,
                )))
            }

            GraftPragma::Fork => {
                let snapshot = file.snapshot_or_latest()?;
                let missing = runtime.missing_pages(&snapshot).or_into_ctx()?;
                if missing.is_empty() {
                    let graft = runtime.fork(&snapshot).or_into_ctx()?;
                    Ok(Some(format!(
                        "Forked current snapshot into Graft: {}",
                        graft.local,
                    )))
                } else {
                    Ok(Some("ERROR: must hydrate volume before forking".into()))
                }
            }

            GraftPragma::Switch { graft, remote } => {
                let graft = file
                    .handle_mut()
                    .switch_graft(graft, remote)
                    .or_into_ctx()?;
                Ok(Some(format!(
                    "Switched to Graft {} with remote Volume {}",
                    graft.local, graft.remote,
                )))
            }

            GraftPragma::Info => Ok(Some(format_graft_info(file)?)),
            GraftPragma::Status => Ok(Some(format_graft_status(file)?)),

            GraftPragma::Snapshot => {
                let snapshot = file.snapshot_or_latest()?;
                Ok(Some(format!("{snapshot:?}")))
            }

            GraftPragma::Fetch => Ok(Some(fetch_or_pull(file, false)?)),
            GraftPragma::Pull => Ok(Some(fetch_or_pull(file, true)?)),

            GraftPragma::Push => Ok(Some(push(file)?)),

            GraftPragma::Audit => Ok(Some(format_graft_audit(runtime, file)?)),

            GraftPragma::Hydrate => {
                file.handle().hydrate().or_into_ctx()?;
                Ok(None)
            }

            GraftPragma::Version => {
                const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
                const GITHUB_SHA: Option<&str> = option_env!("GITHUB_SHA");
                let mut out = format!("Graft Version: {PKG_VERSION}");
                if let Some(sha) = GITHUB_SHA {
                    writeln!(&mut out, "\nGit Commit: {sha}")?;
                }
                Ok(Some(out))
            }

            GraftPragma::Import(path) => graft_import(file.handle(), path).map(Some),

            GraftPragma::DumpSqliteHeader => {
                let page = file
                    .handle()
                    .reader()
                    .or_into_ctx()?
                    .read_page(PageIdx::FIRST)
                    .or_into_ctx()?;
                let header = SqliteHeader::read_from_bytes(&page[..100])
                    .expect("failed to parse SQLite header");
                Ok(Some(format!("{header:#?}")))
            }
        }
    }
}

macro_rules! pluralize {
    ($n:expr, $s:literal) => {
        if $n == 1 { $s } else { concat!($s, "s") }
    };
}

fn format_graft_info(file: &VolFile) -> Result<String, Culprit<ErrCtx>> {
    let state = file.handle().state().or_into_ctx()?;
    let sync = state.sync().map_or_else(
        || "Never synced".into(),
        |sync| match sync.local_watermark {
            Some(local) => format!("L{local} -> R{}", sync.remote),
            None => format!("R{}", sync.remote),
        },
    );
    let local = state.local;
    let remote = state.remote;
    let snapshot = file.snapshot_or_latest()?;
    let page_count = file.page_count()?;
    let vol_size = PAGESIZE * page_count.to_usize();

    Ok(formatdoc!(
        "
            Graft: {local}
            Remote: {remote}
            Last sync: {sync}
            Snapshot: {snapshot:?}
            Snapshot pages: {page_count}
            Snapshot size: {vol_size}
        "
    ))
}

fn format_graft_status(file: &VolFile) -> Result<String, Culprit<ErrCtx>> {
    let mut f = String::new();

    let tag = file.handle().tag();
    writeln!(&mut f, "On tag {tag}")?;

    let status = file.handle().status().or_into_ctx()?;
    let local_changes = status.local_status.changes();
    let remote_changes = status.remote_status.changes();

    writeln!(
        &mut f,
        indoc! {"
            Local Volume {} ({}) is grafted to
            remote Volume {} ({}).
        "},
        status.local, status.local_status, status.remote, status.remote_status,
    )?;

    match (local_changes, remote_changes) {
        (Some(local), Some(remote)) => {
            write!(
                &mut f,
                indoc! {"
                    The local and remote Volumes have diverged, and have {} and {}
                    different commits each, respectively.
                "},
                local.len(),
                remote.len(),
            )?;
        }
        (Some(local), None) => {
            write!(
                &mut f,
                indoc! {"
                    The local Volume is {} {} ahead of the remote Volume.
                      (use 'pragma graft_push' to push changes)
                "},
                local.len(),
                pluralize!(local.len(), "commit")
            )?;
        }
        (None, Some(remote)) => {
            writeln!(
                &mut f,
                indoc! {"
                    The remote Volume is {} {} ahead of the local Volume.
                      (use 'pragma graft_pull' to pull changes)
                "},
                remote.len(),
                pluralize!(remote.len(), "commit")
            )?;
        }
        (None, None) => {
            write!(
                &mut f,
                "The local Volume is up to date with the remote Volume."
            )?;
        }
    }

    Ok(f)
}

fn format_graft_audit(runtime: &Runtime, file: &VolFile) -> Result<String, Culprit<ErrCtx>> {
    let snapshot = file.snapshot_or_latest()?;
    let missing_pages = runtime.missing_pages(&snapshot).or_into_ctx()?;
    let pages = file.page_count().or_into_ctx()?.to_usize();
    if missing_pages.is_empty() {
        let checksum = runtime.checksum(&snapshot).or_into_ctx()?;
        Ok(formatdoc!(
            "
                Cached {pages} of {pages} {} (100%%) from the remote volume.
                Checksum: {checksum}
            ",
            pluralize!(pages, "page"),
        ))
    } else {
        let missing = missing_pages.cardinality();
        let have = pages - missing;
        let pct = (have as f64) / (pages as f64) * 100.0;
        Ok(formatdoc!(
            "
                Cached {have} of {pages} {} ({pct:.02}%%) from the remote volume.
                  (use 'pragma graft_hydrate' to fetch missing pages)
            ",
            pluralize!(pages, "page"),
        ))
    }
}

fn fetch_or_pull(file: &mut VolFile, pull: bool) -> Result<String, Culprit<ErrCtx>> {
    let pre = file.handle().status().or_into_ctx()?;
    if pull {
        file.handle().pull().or_into_ctx()?;
    } else {
        file.handle().fetch().or_into_ctx()?;
    }
    let post = file.handle().status().or_into_ctx()?;

    let mut f = String::new();

    if let Some(diff) = AheadStatus::new(post.remote_status.head, pre.remote_status.head).changes()
    {
        writeln!(
            &mut f,
            "Pulled LSNs {} into remote Volume {}",
            diff.to_string(),
            post.remote
        )?;
    } else {
        writeln!(&mut f, "No changes to remote Volume {}", post.remote)?;
    }

    if pull {
        if let Some(diff) =
            AheadStatus::new(post.local_status.head, pre.local_status.head).changes()
        {
            writeln!(
                &mut f,
                "Pulled LSNs {} into local Volume {}",
                diff.to_string(),
                post.remote
            )?;
        } else {
            writeln!(&mut f, "No changes to local Volume {}", post.remote)?;
        }
    }

    Ok(f)
}

fn push(file: &mut VolFile) -> Result<String, Culprit<ErrCtx>> {
    let pre = file.handle().status().or_into_ctx()?;
    if let Some(changes) = pre.local_status.changes()
        && !changes.is_empty()
    {
        file.handle().push().or_into_ctx()?;
        let post = file.handle().status().or_into_ctx()?;

        let pushed = AheadStatus::new(post.local_status.base, pre.local_status.base).changes();

        Ok(formatdoc!(
            "
                Pushed LSNs {} from local Volume {}
                to remote Volume {} @ {}
            ",
            pushed.map_or("unknown".into(), |lsns| lsns.to_string()),
            post.local,
            post.remote,
            post.remote_status
                .base
                .map_or("unknown".into(), |l| l.to_string())
        ))
    } else {
        Ok("Everything up-to-date".to_string())
    }
}

fn format_tags(runtime: &Runtime) -> Result<String, Culprit<ErrCtx>> {
    let mut f = String::new();
    let mut tags = runtime.iter_tags();
    while let Some((tag, graft)) = tags.try_next().or_into_ctx()? {
        let handle = runtime.get_or_create_tag(&tag).or_into_ctx()?;
        let status = handle.status().or_into_ctx()?;
        let remote = handle.remote().or_into_ctx()?;

        writedoc!(
            &mut f,
            "
                Tag: {tag}
                  Graft: {graft}
                    Remote: {remote}
                    Status: {status}
            ",
        )?;
    }
    Ok(f)
}

fn format_grafts(runtime: &Runtime, current_graft: &VolumeId) -> Result<String, Culprit<ErrCtx>> {
    let mut f = String::new();
    let mut grafts = runtime.iter_grafts();
    while let Some(graft) = grafts.try_next().or_into_ctx()? {
        let status = runtime.graft_status(&graft.local).or_into_ctx()?;
        let local = graft.local;
        let remote = graft.remote;

        writedoc!(
            &mut f,
            "
                Graft: {local}{}
                  Remote: {remote}
                  Status: {status}
            ",
            if &local == current_graft {
                " (current)"
            } else {
                ""
            }
        )?;
    }
    Ok(f)
}

fn graft_import(handle: &TagHandle, path: PathBuf) -> Result<String, Culprit<ErrCtx>> {
    let mut writer = handle.writer().or_into_ctx()?;
    if writer.page_count().or_into_ctx()? > PageCount::ZERO {
        return Ok("Refusing to import into a non-empty database.".into());
    }

    let mut file = File::open(&path)?;

    // Read and write the file in chunks of 64 pages (256KB)
    const CHUNK_PAGES: usize = 64;
    const CHUNK_SIZE: usize = CHUNK_PAGES * PAGESIZE.as_usize();

    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut page_idx = PageIdx::FIRST;
    let mut total_pages = 0;

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break; // EOF
        }

        // Process each page in the chunk
        for chunk in buffer[..bytes_read].chunks(PAGESIZE.as_usize()) {
            let page = if chunk.len() == PAGESIZE.as_usize() {
                // SAFETY: we just checked that chunk.len() == PAGESIZE
                unsafe { Page::from_bytes_unchecked(Bytes::copy_from_slice(chunk)) }
            } else {
                // Partial page at the end of the file - pad with zeros
                let mut bytes = BytesMut::from(chunk);
                bytes.resize(PAGESIZE.as_usize(), 0);
                // SAFETY: chunk has just been resized to PAGESIZE
                unsafe { Page::from_bytes_unchecked(bytes.freeze()) }
            };

            writer.write_page(page_idx, page).or_into_ctx()?;
            page_idx = page_idx.saturating_next();
            total_pages += 1;
        }
    }

    let reader = writer.commit().or_into_ctx()?;
    let page_count = reader.page_count().or_into_ctx()?;
    assert_eq!(
        page_count.to_usize(),
        total_pages,
        "page count after import does not match expected page count"
    );

    Ok(format!(
        "imported {} {}",
        total_pages,
        pluralize!(total_pages, "page")
    ))
}
