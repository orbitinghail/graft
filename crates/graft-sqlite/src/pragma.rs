use culprit::ResultExt;
use graft_client::runtime::{fetcher::Fetcher, runtime::Runtime};
use sqlite_plugin::vfs::Pragma;

use crate::{file::vol_file::VolFile, vfs::ErrCtx};

pub enum GraftPragma {
    Status,
}

pub enum PragmaParseErr<'a> {
    Invalid(Pragma<'a>),
    Unknown(Pragma<'a>),
}

impl<'a> TryFrom<Pragma<'a>> for GraftPragma {
    type Error = PragmaParseErr<'a>;

    fn try_from(p: Pragma<'a>) -> Result<Self, Self::Error> {
        if let Some((prefix, suffix)) = p.name.split_once("_") {
            if prefix == "graft" {
                return match suffix {
                    "status" => Ok(GraftPragma::Status),
                    _ => Err(PragmaParseErr::Invalid(p)),
                };
            }
        }
        Err(PragmaParseErr::Unknown(p))
    }
}

impl GraftPragma {
    pub fn eval<F: Fetcher>(
        self,
        _runtime: &Runtime<F>,
        file: &mut VolFile,
    ) -> culprit::Result<Option<String>, ErrCtx> {
        match self {
            GraftPragma::Status => {
                if let Some(snapshot) = file.handle().snapshot().or_into_ctx()? {
                    Ok(Some(format!("Current snapshot: {snapshot}")))
                } else {
                    Ok(Some(format!("No snapshot")))
                }
            }
        }
    }
}
