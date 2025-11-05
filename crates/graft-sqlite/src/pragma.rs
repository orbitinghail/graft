use std::fmt::Write;

use culprit::{Culprit, ResultExt};
use graft_kernel::{
    page_status::PageStatus, rt::runtime_handle::RuntimeHandle, volume_reader::VolumeRead,
};
use sqlite_plugin::{
    vars::SQLITE_ERROR,
    vfs::{Pragma, PragmaErr},
};

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
        if let Some((prefix, suffix)) = p.name.split_once("_")
            && prefix == "graft"
        {
            return match suffix {
                "status" => Ok(GraftPragma::Status),
                "snapshot" => Ok(GraftPragma::Snapshot),
                "pages" => Ok(GraftPragma::Pages),
                "pull" => Ok(GraftPragma::Pull),
                "reset" => Ok(GraftPragma::Reset),
                "sync_errors" => Ok(GraftPragma::SyncErrors),
                "version" => Ok(GraftPragma::Version),
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
        _runtime: &RuntimeHandle,
        file: &mut VolFile,
    ) -> Result<Option<String>, Culprit<ErrCtx>> {
        match self {
            GraftPragma::Status => {
                let snapshot = file.snapshot_or_latest()?;
                let page_count = file.page_count()?;
                let status = file.handle().status().or_into_ctx()?;
                let mut out = "Graft Status\n".to_string();
                writeln!(&mut out, "Snapshot: {snapshot:?}")?;
                writeln!(&mut out, "Page Count: {page_count}")?;
                writeln!(&mut out, "Volume status: {status}",)?;
                Ok(Some(out))
            }

            GraftPragma::Snapshot => {
                let snapshot = file.snapshot_or_latest()?;
                Ok(Some(format!("{snapshot:?}")))
            }

            GraftPragma::Pages => {
                let mut out = format!("{:<8} | {:<6} | state\n", "pageno", "lsn");
                let reader = file.reader()?;
                let pages = reader.page_count().or_into_ctx()?;

                for pageidx in pages.iter() {
                    write!(&mut out, "{:<8} | ", pageidx.to_u32())?;
                    match reader.page_status(pageidx).or_into_ctx()? {
                        PageStatus::Pending(lsn) => {
                            writeln!(&mut out, "{lsn:<6} | pending")?;
                        }
                        PageStatus::Empty(lsn) => {
                            writeln!(
                                &mut out,
                                "{} | empty",
                                match lsn {
                                    Some(lsn) => format!("{:<6}", lsn),
                                    None => format!("{:<6}", "_"),
                                }
                            )?;
                        }
                        PageStatus::Available(lsn) => {
                            writeln!(&mut out, "{lsn:<6} | available")?;
                        }
                        PageStatus::Dirty => writeln!(&mut out, "{:<6} | dirty", "_")?,
                    }
                }

                Ok(Some(out))
            }
            GraftPragma::Pull => {
                todo!("pull all of the pages accessible by the current or latest snapshot")
            }
            GraftPragma::SyncErrors => todo!("list recent sync errors"),
            GraftPragma::Reset => todo!("reset the volume to the remote"),

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
