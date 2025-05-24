use culprit::{Culprit, ResultExt};
use graft_client::runtime::{
    runtime::Runtime, storage::page::PageStatus, volume_reader::VolumeRead,
};
use sqlite_plugin::{
    vars::SQLITE_ERROR,
    vfs::{Pragma, PragmaErr},
};
use std::{fmt::Write, time::Instant};

use crate::{file::vol_file::VolFile, vfs::ErrCtx};

pub enum GraftPragma {
    /// `pragma graft_status;`
    Status,

    /// `pragma graft_snapshot;`
    Snapshot,

    /// `pragma graft_pages;`
    Pages,

    /// `pragma graft_pull;`
    Pull,

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
                    "pull" => Ok(GraftPragma::Pull),
                    "reset" => Ok(GraftPragma::Reset),
                    "sync" => {
                        let arg = p.arg.ok_or(PragmaErr::required_arg(p))?;
                        let autosync = arg.parse().map_err(|err| {
                            PragmaErr::Fail(SQLITE_ERROR, Some(format!("{err:?}")))
                        })?;
                        Ok(GraftPragma::SetAutosync(autosync))
                    }
                    "sync_errors" => Ok(GraftPragma::SyncErrors),
                    "version" => Ok(GraftPragma::Version),
                    _ => Err(PragmaErr::Fail(
                        SQLITE_ERROR,
                        Some(format!("invalid graft pragma `{}`", p.name)),
                    )),
                };
            }
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
            GraftPragma::Status => {
                let mut out = "Graft Status\n".to_string();
                writeln!(&mut out, "Client ID: {}", runtime.cid())?;
                writeln!(&mut out, "Volume ID: {}", file.vid())?;
                if let Some(snapshot) = file.snapshot_or_latest()? {
                    writeln!(&mut out, "Current snapshot: {snapshot}")?;
                } else {
                    writeln!(&mut out, "Current snapshot: None")?;
                }
                writeln!(&mut out, "Autosync: {}", runtime.get_autosync())?;
                writeln!(
                    &mut out,
                    "Volume status: {:?}",
                    file.handle().status().or_into_ctx()?
                )?;
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
                let reader = file.reader()?;

                macro_rules! fmt_lsn {
                    ($lsn:expr) => {
                        match $lsn {
                            Some(lsn) => format!("{:<6}", lsn),
                            None => format!("{:<6}", "_"),
                        }
                    };
                }

                let snapshot_lsn = reader.snapshot().map(|s| s.local());
                let pages = reader.snapshot().map(|s| s.pages()).unwrap_or_default();
                for pageidx in pages.iter() {
                    write!(&mut out, "{:<8} | ", pageidx.to_u32())?;

                    let status = reader.status(pageidx).or_into_ctx()?;
                    match status {
                        PageStatus::Pending => {
                            writeln!(&mut out, "{} | pending", fmt_lsn!(snapshot_lsn))?
                        }
                        PageStatus::Empty(lsn) => writeln!(&mut out, "{} | empty", fmt_lsn!(lsn))?,
                        PageStatus::Available(lsn) => {
                            writeln!(&mut out, "{} | available", fmt_lsn!(Some(lsn)))?
                        }
                        PageStatus::Dirty => writeln!(&mut out, "{:<6} | dirty", "_")?,
                    }
                }
                Ok(Some(out))
            }
            GraftPragma::Pull => {
                file.pull()?;
                Ok(None)
            }
            GraftPragma::Reset => {
                file.handle().reset_to_remote().or_into_ctx()?;
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
