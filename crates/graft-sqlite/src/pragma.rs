use std::{
    fmt::Write,
    fs::File,
    io::{Read, Write as IoWrite},
    path::PathBuf,
};

use bytes::{Bytes, BytesMut};
use graft::core::{
    LogId, PageCount, PageIdx, VolumeId,
    lsn::LSNRangeExt,
    page::{PAGESIZE, Page},
};
use graft::{
    rt::runtime::Runtime,
    volume::AheadStatus,
    volume_reader::VolumeRead,
    volume_writer::{VolumeWrite, VolumeWriter},
};
use indoc::{formatdoc, indoc, writedoc};
use itertools::Itertools;
use sqlite_plugin::{
    vars::SQLITE_ERROR,
    vfs::{Pragma, PragmaErr},
};
use tryiter::TryIteratorExt;
use zerocopy::FromBytes;

use crate::{dbg::SqliteHeader, file::vol_file::VolFile, vfs::ErrCtx};

pub enum GraftPragma {
    /// `pragma graft_volumes;`
    Volumes,

    /// `pragma graft_tags;`
    Tags,

    /// `pragma graft_switch = "local_vid[:local[:remote]]";`
    Switch {
        vid: VolumeId,
        local: Option<LogId>,
        remote: Option<LogId>,
    },

    /// `pragma graft_clone [= "remote"];`
    Clone { remote: Option<LogId> },

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

    /// `pragma graft_export = "PATH";`
    Export(PathBuf),

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
                "volumes" => Ok(GraftPragma::Volumes),
                "tags" => Ok(GraftPragma::Tags),
                "clone" => {
                    if let Some(arg) = p.arg {
                        let remote = arg
                            .parse::<LogId>()
                            .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))?;
                        Ok(GraftPragma::Clone { remote: Some(remote) })
                    } else {
                        Ok(GraftPragma::Clone { remote: None })
                    }
                }
                "fork" => Ok(GraftPragma::Fork),
                "new" => Ok(GraftPragma::Switch {
                    vid: VolumeId::random(),
                    local: None,
                    remote: None,
                }),
                "switch" => {
                    let arg = p.arg.ok_or_else(|| PragmaErr::required_arg(p))?;
                    let parts = arg.split(":").collect_vec();

                    if parts.is_empty() || parts.len() > 3 {
                        return Err(PragmaErr::Fail(
                            SQLITE_ERROR,
                            Some(
                                "argument must be in the form: `local_vid[:local[:remote]]`"
                                    .to_string(),
                            ),
                        ));
                    }

                    let vid = parts[0]
                        .parse::<VolumeId>()
                        .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))?;
                    let local = parts
                        .get(1)
                        .map(|s| {
                            s.parse::<LogId>()
                                .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))
                        })
                        .transpose()?;
                    let remote = parts
                        .get(2)
                        .map(|s| {
                            s.parse::<LogId>()
                                .map_err(|err| PragmaErr::Fail(SQLITE_ERROR, Some(err.to_string())))
                        })
                        .transpose()?;

                    Ok(GraftPragma::Switch { vid, local, remote })
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
                "export" => {
                    let arg = p.arg.ok_or_else(|| PragmaErr::required_arg(p))?;
                    let path = PathBuf::from(arg);
                    Ok(GraftPragma::Export(path))
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

macro_rules! pragma_err {
    ($msg:expr) => {
        Err(ErrCtx::PragmaErr($msg.into()))
    };
}

impl GraftPragma {
    pub fn eval(self, runtime: &Runtime, file: &mut VolFile) -> Result<Option<String>, ErrCtx> {
        match self {
            GraftPragma::Volumes => Ok(Some(format_volumes(runtime, file)?)),
            GraftPragma::Tags => Ok(Some(format_tags(runtime, file)?)),

            GraftPragma::Clone { remote } => {
                if !file.is_idle() {
                    return pragma_err!("cannot clone while there is an open transaction");
                }

                let remote = match remote {
                    Some(remote) => remote,
                    None => runtime.volume_get(&file.vid)?.remote,
                };
                let volume = runtime.volume_open(None, None, Some(remote))?;
                file.switch_volume(&volume.vid)?;

                Ok(Some(format!(
                    "Created new Volume {} from remote Log {}",
                    volume.vid, volume.remote
                )))
            }

            GraftPragma::Fork => {
                if !file.is_idle() {
                    return pragma_err!("cannot fork while there is an open transaction");
                }

                let snapshot = file.snapshot_or_latest()?;
                let missing = runtime.snapshot_missing_pages(&snapshot)?;
                if missing.is_empty() {
                    let volume = runtime.volume_from_snapshot(&snapshot)?;
                    file.switch_volume(&volume.vid)?;

                    Ok(Some(format!(
                        "Forked current snapshot into Volume: {}",
                        volume.vid,
                    )))
                } else {
                    pragma_err!("ERROR: must hydrate volume before forking")
                }
            }

            GraftPragma::Switch { vid, local, remote } => {
                if !file.is_idle() {
                    return pragma_err!("cannot switch while there is an open transaction");
                }

                let volume = runtime.volume_open(Some(vid), local, remote)?;
                file.switch_volume(&volume.vid)?;

                Ok(Some(format!(
                    "Switched to Volume {} with local Log {} and remote Log {}",
                    volume.vid, volume.local, volume.remote,
                )))
            }

            GraftPragma::Info => Ok(Some(format_volume_info(runtime, file)?)),
            GraftPragma::Status => Ok(Some(format_volume_status(runtime, file)?)),

            GraftPragma::Snapshot => {
                let snapshot = file.snapshot_or_latest()?;
                Ok(Some(format!("{snapshot:?}")))
            }

            GraftPragma::Fetch => Ok(Some(fetch_or_pull(runtime, file, false)?)),
            GraftPragma::Pull => Ok(Some(fetch_or_pull(runtime, file, true)?)),

            GraftPragma::Push => Ok(Some(push(runtime, file)?)),

            GraftPragma::Audit => Ok(Some(format_volume_audit(runtime, file)?)),

            GraftPragma::Hydrate => {
                let snapshot = file.snapshot_or_latest()?;
                runtime.snapshot_hydrate(snapshot)?;
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

            GraftPragma::Import(path) => {
                let writer = runtime.volume_writer(file.vid.clone())?;
                volume_import(writer, path).map(Some)
            }

            GraftPragma::Export(path) => volume_export(runtime, file, path).map(Some),

            GraftPragma::DumpSqliteHeader => {
                let reader = runtime.volume_reader(file.vid.clone())?;
                let page = reader.read_page(PageIdx::FIRST)?;
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

fn format_volume_info(runtime: &Runtime, file: &VolFile) -> Result<String, ErrCtx> {
    let state = runtime.volume_get(&file.vid)?;
    let sync = state.sync().map_or_else(
        || "Never synced".into(),
        |sync| match sync.local_watermark {
            Some(local) => format!("L{local} -> R{}", sync.remote),
            None => format!("R{}", sync.remote),
        },
    );
    let vid = state.vid;
    let local = state.local;
    let remote = state.remote;
    let snapshot = file.snapshot_or_latest()?;
    let page_count = file.page_count()?;
    let snapshot_size = PAGESIZE * page_count.to_usize();

    Ok(formatdoc!(
        "
            Volume: {vid}
            Local: {local}
            Remote: {remote}
            Last sync: {sync}
            Snapshot: {snapshot:?}
            Snapshot pages: {page_count}
            Snapshot size: {snapshot_size}
        "
    ))
}

fn format_volume_status(runtime: &Runtime, file: &VolFile) -> Result<String, ErrCtx> {
    let mut f = String::new();

    let tag = &file.tag;
    writeln!(&mut f, "On tag {tag}")?;

    let status = runtime.volume_status(&file.vid)?;
    let local_changes = status.local_status.changes();
    let remote_changes = status.remote_status.changes();

    writeln!(
        &mut f,
        indoc! {"
            Local Log {} is grafted to
            remote Log {}.
        "},
        status.local, status.remote,
    )?;

    match (local_changes, remote_changes) {
        (Some(local), Some(remote)) => {
            write!(
                &mut f,
                indoc! {"
                    The Volume and the remote have diverged,
                    and have {} and {} different commits each, respectively.
                "},
                local.len(),
                remote.len(),
            )?;
        }
        (Some(local), None) => {
            write!(
                &mut f,
                indoc! {"
                    The Volume is ahead of the remote by {} {}.
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
                    The Volume is behind the remote by {} {}.
                      (use 'pragma graft_pull' to pull changes)
                "},
                remote.len(),
                pluralize!(remote.len(), "commit")
            )?;
        }
        (None, None) => {
            write!(&mut f, "The Volume is up to date with the remote.")?;
        }
    }

    Ok(f)
}

fn format_volume_audit(runtime: &Runtime, file: &VolFile) -> Result<String, ErrCtx> {
    let snapshot = file.snapshot_or_latest()?;
    let missing_pages = runtime.snapshot_missing_pages(&snapshot)?;
    let pages = file.page_count()?.to_usize();
    if missing_pages.is_empty() {
        let checksum = runtime.snapshot_checksum(&snapshot)?;
        Ok(formatdoc!(
            "
                Cached {pages} of {pages} {} (100%%) from the remote Log.
                Checksum: {checksum}
            ",
            pluralize!(pages, "page"),
        ))
    } else {
        let missing = missing_pages.cardinality().to_usize();
        let have = pages - missing;
        let pct = (have as f64) / (pages as f64) * 100.0;
        Ok(formatdoc!(
            "
                Cached {have} of {pages} {} ({pct:.02}%%) from the remote Log.
                  (use 'pragma graft_hydrate' to fetch missing pages)
            ",
            pluralize!(pages, "page"),
        ))
    }
}

fn fetch_or_pull(runtime: &Runtime, file: &mut VolFile, pull: bool) -> Result<String, ErrCtx> {
    let pre = runtime.volume_status(&file.vid)?;
    if pull {
        runtime.volume_pull(file.vid.clone())?;
    } else {
        runtime.fetch_log(pre.remote, None)?;
    }
    let post = runtime.volume_status(&file.vid)?;

    let mut f = String::new();

    if let Some(diff) = AheadStatus::new(post.remote_status.base, pre.remote_status.base).changes()
    {
        writeln!(
            &mut f,
            "Pulled LSNs {} into remote Log {}",
            diff.to_string(),
            post.remote
        )?;
    } else {
        writeln!(&mut f, "No changes to remote Log {}", post.remote)?;
    }

    Ok(f)
}

fn push(runtime: &Runtime, file: &mut VolFile) -> Result<String, ErrCtx> {
    let pre = runtime.volume_status(&file.vid)?;
    if let Some(changes) = pre.local_status.changes()
        && !changes.is_empty()
    {
        runtime.volume_push(file.vid.clone())?;
        let post = runtime.volume_status(&file.vid)?;

        let pushed = AheadStatus::new(post.local_status.base, pre.local_status.base).changes();

        Ok(formatdoc!(
            "
                Pushed LSNs {} from local Log {}
                to remote Log {} @ {}
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

fn format_tags(runtime: &Runtime, file: &VolFile) -> Result<String, ErrCtx> {
    let mut f = String::new();
    let mut tags = runtime.tag_iter();
    while let Some((tag, vid)) = tags.try_next()? {
        let status = runtime.volume_status(&vid)?;
        let local = &status.local;
        let remote = &status.remote;

        writedoc!(
            &mut f,
            "
                Tag: {tag}{}
                  Volume: {vid}
                    Local: {local}
                    Remote: {remote}
                    Status: {status}
            ",
            if tag == file.tag { " (current)" } else { "" }
        )?;
    }
    Ok(f)
}

fn format_volumes(runtime: &Runtime, file: &VolFile) -> Result<String, ErrCtx> {
    let mut f = String::new();
    let mut volumes = runtime.volume_iter();
    while let Some(volume) = volumes.try_next()? {
        let vid = volume.vid;
        let status = runtime.volume_status(&vid)?;
        let local = volume.local;
        let remote = volume.remote;

        writedoc!(
            &mut f,
            "
                Volume: {vid}{}
                  Local: {local}
                  Remote: {remote}
                  Status: {status}
            ",
            if vid == file.vid { " (current)" } else { "" }
        )?;
    }
    Ok(f)
}

fn volume_import(mut writer: VolumeWriter, path: PathBuf) -> Result<String, ErrCtx> {
    if writer.page_count() > PageCount::ZERO {
        return pragma_err!("Refusing to import into a non-empty database.");
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

            writer.write_page(page_idx, page)?;
            page_idx = page_idx.saturating_next();
            total_pages += 1;
        }
    }

    let reader = writer.commit()?;
    let page_count = reader.page_count();
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

fn volume_export(_runtime: &Runtime, file: &VolFile, path: PathBuf) -> Result<String, ErrCtx> {
    // Get a reader based on the current state of the VolFile
    let reader = file.reader()?;

    let page_count = reader.page_count();
    let total_pages = page_count.to_usize();

    let mut output_file = File::create(&path)?;

    // Iterate over all pages and write them to the output file
    for page_idx in page_count.iter() {
        let page = reader.read_page(page_idx)?;
        output_file.write_all(page.as_ref())?;
    }

    Ok(format!(
        "exported {} {}",
        total_pages,
        pluralize!(total_pages, "page")
    ))
}
