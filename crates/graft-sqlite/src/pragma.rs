use graft_client::runtime::{
    runtime::Runtime, storage::page::PageValue, volume_reader::VolumeRead,
};
use sqlite_plugin::vfs::{Pragma, PragmaErr};
use std::{fmt::Write, time::Instant};

use crate::file::vol_file::VolFile;

pub enum GraftPragma {
    /// `pragma graft_status;`
    Status,

    /// `pragma graft_snapshot;`
    Snapshot,

    /// `pragma graft_pages;`
    Pages,

    /// `pragma graft_sync = true|false;`
    SetAutosync(bool),

    /// `pragma graft_sync_errors;`
    SyncErrors,

    /// `pragma graft_reset;`
    Reset,

    /// `pragma graft_version;`
    Version,
}

impl TryFrom<&Pragma<'_>> for GraftPragma {
    type Error = PragmaErr;

    fn try_from(p: &Pragma<'_>) -> Result<Self, Self::Error> {
        if let Some((prefix, suffix)) = p.name.split_once("_") {
            if prefix == "graft" {
                return match suffix {
                    "status" => Ok(GraftPragma::Status),
                    "snapshot" => Ok(GraftPragma::Snapshot),
                    "pages" => Ok(GraftPragma::Pages),
                    "reset" => Ok(GraftPragma::Reset),
                    "sync" => {
                        let arg = p.arg.ok_or(PragmaErr::required_arg(p))?;
                        let autosync = arg.parse()?;
                        Ok(GraftPragma::SetAutosync(autosync))
                    }
                    "sync_errors" => Ok(GraftPragma::SyncErrors),
                    "version" => Ok(GraftPragma::Version),
                    _ => Err(PragmaErr::Fail(format!(
                        "invalid graft pragma `{}`",
                        p.name
                    ))),
                };
            }
        }
        Err(PragmaErr::NotFound)
    }
}

impl GraftPragma {
    pub fn eval(self, runtime: &Runtime, file: &mut VolFile) -> Result<Option<String>, PragmaErr> {
        match self {
            GraftPragma::Status => {
                let mut out = "Graft Status\n".to_string();
                writeln!(&mut out, "Client ID: {}", runtime.cid())?;
                writeln!(&mut out, "Volume ID: {}", file.handle().vid())?;
                if let Some(snapshot) = file.snapshot_or_latest()? {
                    writeln!(&mut out, "Current snapshot: {snapshot}")?;
                } else {
                    writeln!(&mut out, "Current snapshot: None")?;
                }
                writeln!(&mut out, "Autosync: {}", runtime.get_autosync())?;
                writeln!(&mut out, "Volume status: {:?}", file.handle().status()?)?;
                Ok(Some(out))
            }
            GraftPragma::SyncErrors => {
                let sync_errs = runtime.drain_recent_sync_errors();
                let mut out = "Recent sync errors:\n".to_string();
                for (when, err) in sync_errs {
                    let since = Instant::now() - when;
                    writeln!(&mut out, "{}s ago: {}", since.as_secs(), err.ctx())?;
                    for (i, frame) in err.trace().iter().enumerate() {
                        writeln!(&mut out, "  {}: {frame}", i + 1)?;
                    }
                }
                Ok(Some(out))
            }

            GraftPragma::Snapshot => Ok(file.snapshot_or_latest()?.map(|s| s.to_string())),
            GraftPragma::SetAutosync(autosync) => {
                runtime.set_autosync(autosync);
                Ok(None)
            }
            GraftPragma::Pages => {
                let mut out = format!("{:<8} | {:<6} | state\n", "pageno", "lsn");
                let reader = file.handle().reader()?;
                let pages = reader.snapshot().map(|s| s.pages()).unwrap_or_default();
                for pageidx in pages.iter() {
                    let (lsn, page) = reader.read_cached(pageidx)?;
                    writeln!(
                        &mut out,
                        "{:<8} | {:<6} | {}",
                        pageidx.to_u32(),
                        match lsn {
                            Some(lsn) => lsn.to_string(),
                            None => "None".to_string(),
                        },
                        match page {
                            PageValue::Pending => "pending",
                            PageValue::Empty => "empty",
                            PageValue::Available(_) => "cached",
                        }
                    )?;
                }
                Ok(Some(out))
            }
            GraftPragma::Reset => {
                file.handle().reset_to_remote()?;
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
        }
    }
}
