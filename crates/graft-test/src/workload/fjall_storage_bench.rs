use culprit::Culprit;
use graft_tracing::running_in_antithesis;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::workload::{Workload, WorkloadEnv, WorkloadErr};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FjallStorageBench;

impl Workload for FjallStorageBench {
    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>> {
        use std::process::Command;

        // let antithesis control how many/often this workload runs
        env.ticker.finish();

        let duration = if running_in_antithesis() {
            env.rng.random_range(5..900).to_string()
        } else {
            "5".to_string()
        };

        tracing::info!("Running fjall-storage-bench for {} seconds", duration);

        enum Arg<'a> {
            Pos(&'a str),
            Flg(&'a str, &'a str),
        }

        let args = [
            Arg::Pos("run"),
            Arg::Flg("--compression", "none"),
            Arg::Flg("--backend", "fjall-nightly"),
            Arg::Flg("--data-dir", ".data"),
            Arg::Flg("--cache-size", "33554432"),
            Arg::Flg("--seconds", &duration),
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

        let mut handle = match cmd.spawn() {
            Ok(handle) => handle,
            Err(err) => {
                precept::expect_unreachable!("rust-storage-bench failed to start", { "err": err.to_string() });
                return Ok(());
            }
        };

        match handle.wait() {
            Ok(status) => {
                precept::expect_always!(status.success(), "rust-storage-bench command runs")
            }
            Err(err) => {
                precept::expect_unreachable!("rust-storage-bench failed to finish", { "err": err.to_string() });
            }
        };

        Ok(())
    }
}
