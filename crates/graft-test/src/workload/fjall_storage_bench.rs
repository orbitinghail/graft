use culprit::Culprit;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::workload::{Workload, WorkloadEnv, WorkloadErr};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FjallStorageBench;

impl Workload for FjallStorageBench {
    fn run<R: Rng>(&mut self, _env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>> {
        use std::process::Command;

        enum Arg {
            Pos(&'static str),
            Flg(&'static str, &'static str),
        }

        let args = [
            Arg::Pos("run"),
            Arg::Flg("--compression", "none"),
            Arg::Flg("--backend", "fjall-nightly"),
            Arg::Flg("--data-dir", ".data"),
            Arg::Flg("--cache-size", "536870912"),
            Arg::Flg("--seconds", "900"),
            Arg::Flg("--out", "fjall-nightly-output.jsonl"),
            Arg::Pos("read-write"),
            Arg::Pos("--write-random"),
            Arg::Flg("--value-size", "0"),
            Arg::Flg("--item-count", "1000"),
        ];

        let mut cmd = Command::new("/rust-storage-bench");
        for arg in &args {
            match arg {
                Arg::Pos(s) => cmd.arg(s),
                Arg::Flg(flag, value) => cmd.arg(flag).arg(value),
            };
        }

        let output = cmd.output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        precept::expect_always!(
            output.status.success(),
            "rust-storage-bench command failed",
            {
                "stdout": String::from(stdout),
                "stderr": String::from(stderr),
            }
        );

        Ok(())
    }
}
