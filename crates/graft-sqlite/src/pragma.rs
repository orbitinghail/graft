use graft_client::runtime::runtime::Runtime;
use sqlite_plugin::vfs::{Pragma, PragmaErr};
use std::fmt::Write;

use crate::file::vol_file::VolFile;

pub enum GraftPragma {
    Status,
    SetAutosync(bool),
}

impl TryFrom<&Pragma<'_>> for GraftPragma {
    type Error = PragmaErr;

    fn try_from(p: &Pragma<'_>) -> Result<Self, Self::Error> {
        if let Some((prefix, suffix)) = p.name.split_once("_") {
            if prefix == "graft" {
                return match suffix {
                    "status" => Ok(GraftPragma::Status),
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
                if let Some(snapshot) = file.handle().snapshot()? {
                    writeln!(&mut out, "Current snapshot: {snapshot}")?;
                } else {
                    writeln!(&mut out, "Current snapshot: None")?;
                }
                writeln!(&mut out, "Autosync: {}", runtime.get_autosync())?;
                Ok(Some(out))
            }
            GraftPragma::SetAutosync(autosync) => {
                runtime.set_autosync(autosync);
                Ok(None)
            }
        }
    }
}
