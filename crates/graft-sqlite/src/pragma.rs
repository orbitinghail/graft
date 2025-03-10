use graft_client::runtime::{
    runtime::Runtime, storage::page::PageValue, volume_reader::VolumeRead,
};
use sqlite_plugin::vfs::{Pragma, PragmaErr};
use std::fmt::Write;

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
                    "sync" => {
                        let arg = p.arg.ok_or(PragmaErr::required_arg(p))?;
                        let autosync = arg.parse()?;
                        Ok(GraftPragma::SetAutosync(autosync))
                    }
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
                if let Some(snapshot) = file.snapshot_or_latest()? {
                    writeln!(&mut out, "Current snapshot: {snapshot}")?;
                } else {
                    writeln!(&mut out, "Current snapshot: None")?;
                }
                writeln!(&mut out, "Autosync: {}", runtime.get_autosync())?;
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
        }
    }
}
