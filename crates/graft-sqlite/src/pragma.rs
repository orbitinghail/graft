use std::fmt::Write;

use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, lsn::LSNRangeExt};
use graft_kernel::{
    graft::AheadStatus, page_status::PageStatus, rt::runtime_handle::RuntimeHandle,
    volume_reader::VolumeRead,
};
use indoc::{formatdoc, indoc, writedoc};
use sqlite_plugin::{
    vars::SQLITE_ERROR,
    vfs::{Pragma, PragmaErr},
};
use tryiter::TryIteratorExt;

use crate::{file::vol_file::VolFile, vfs::ErrCtx};

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

    /// `pragma graft_pages;`
    Pages,

    /// `pragma graft_hydrate;`
    Hydrate,

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
                "status" => Ok(GraftPragma::Status),
                "snapshot" => Ok(GraftPragma::Snapshot),
                "fetch" => Ok(GraftPragma::Fetch),
                "pull" => Ok(GraftPragma::Pull),
                "push" => Ok(GraftPragma::Push),
                "pages" => Ok(GraftPragma::Pages),
                "hydrate" => Ok(GraftPragma::Hydrate),
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
        runtime: &RuntimeHandle,
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

            GraftPragma::Status => Ok(Some(format_graft_status(file)?)),

            GraftPragma::Snapshot => {
                let snapshot = file.snapshot_or_latest()?;
                Ok(Some(format!("{snapshot:?}")))
            }

            GraftPragma::Fetch => Ok(Some(fetch_or_pull(file, false)?)),
            GraftPragma::Pull => Ok(Some(fetch_or_pull(file, true)?)),

            GraftPragma::Push => Ok(Some(push(file)?)),

            GraftPragma::Pages => Ok(Some(format_graft_pages(file)?)),

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
        }
    }
}

macro_rules! pluralize {
    (1, $s:literal) => {
        $s
    };
    ($n:expr, $s:literal) => {
        concat!($s, "s")
    };
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

fn format_graft_pages(file: &VolFile) -> Result<String, Culprit<ErrCtx>> {
    let mut f = format!("{:<8} | {:<6} | state\n", "pageno", "lsn");
    let reader = file.reader()?;
    let pages = reader.page_count().or_into_ctx()?;

    for pageidx in pages.iter() {
        write!(&mut f, "{:<8} | ", pageidx.to_u32())?;
        match reader.page_status(pageidx).or_into_ctx()? {
            PageStatus::Pending(lsn) => {
                writeln!(&mut f, "{lsn:<6} | pending")?;
            }
            PageStatus::Empty(lsn) => {
                writeln!(
                    &mut f,
                    "{} | empty",
                    match lsn {
                        Some(lsn) => format!("{lsn:<6}"),
                        None => format!("{:<6}", "_"),
                    }
                )?;
            }
            PageStatus::Available(lsn) => {
                writeln!(&mut f, "{lsn:<6} | available")?;
            }
            PageStatus::Dirty => writeln!(&mut f, "{:<6} | dirty", "_")?,
        }
    }

    Ok(f)
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

fn format_tags(runtime: &RuntimeHandle) -> Result<String, Culprit<ErrCtx>> {
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

fn format_grafts(
    runtime: &RuntimeHandle,
    current_graft: &VolumeId,
) -> Result<String, Culprit<ErrCtx>> {
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
