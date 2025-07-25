use std::path::Path;

use culprit::Culprit;
use graft_tracing::running_in_antithesis;
use rand::{Rng, seq::IndexedRandom};
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
            env.rng.random_range(150..1500).to_string()
        } else {
            "5".to_string()
        };

        let cache_size = if running_in_antithesis() {
            env.rng.random_range(32..256) * 1024 * 1024
        } else {
            32 * 1024 * 1024
        }
        .to_string();

        let value_size = if running_in_antithesis() {
            if env.rng.random_bool(0.5) {
                env.rng.random_range(0..1024)
            } else {
                0
            }
        } else {
            0
        }
        .to_string();

        let item_count = env.rng.random_range(1000..100000).to_string();

        // delete fjall-nightly-output.jsonl if it exists
        if Path::new("fjall-nightly-output.jsonl").exists() {
            std::fs::remove_file("fjall-nightly-output.jsonl")
                .expect("Failed to remove existing fjall-nightly-output.jsonl");
        }

        tracing::info!("Running fjall-storage-bench for {} seconds", duration);

        enum Arg<'a> {
            Pos(&'a str),
            Flg(&'a str, &'a str),
        }

        let mut args = vec![
            Arg::Pos("run"),
            Arg::Flg("--compression", "none"),
            Arg::Flg("--backend", "fjall-nightly"),
            Arg::Flg("--data-dir", ".data"),
            Arg::Flg("--cache-size", &cache_size),
            Arg::Flg("--seconds", &duration),
            Arg::Flg("--out", "fjall-nightly-output.jsonl"),
        ];

        // pick a random workload
        if env.rng.random_bool(0.5) {
            args.append(&mut vec![
                Arg::Pos("read-write"),
                Arg::Pos("--write-random"),
                Arg::Flg("--value-size", &value_size),
                Arg::Flg("--item-count", &item_count),
            ])
        } else {
            args.append(&mut vec![
                Arg::Pos("ycsb"),
                Arg::Flg("--value-size", &value_size),
                Arg::Flg("--item-count", &item_count),
                Arg::Flg("--type", &["a", "b"].choose(&mut env.rng).unwrap()),
            ])
        }

        let mut cmd = Command::new("/rust-storage-bench");
        let mut args_str = vec![];
        for arg in &args {
            match arg {
                Arg::Pos(s) => {
                    cmd.arg(s);
                    args_str.push(s.to_string());
                }
                Arg::Flg(flag, value) => {
                    cmd.arg(flag).arg(value);
                    args_str.push(format!("{}={}", flag, value));
                }
            };
        }

        let args_str = args_str.join(" ");

        tracing::info!("Running fjall-storage-bench with args: {args_str}",);

        let mut handle = match cmd.spawn() {
            Ok(handle) => handle,
            Err(err) => {
                precept::expect_unreachable!("rust-storage-bench failed to start", {
                    "err": err.to_string(),
                    "args": args_str,
                });
                return Ok(());
            }
        };

        match handle.wait() {
            Ok(status) => {
                precept::expect_always!(status.success(), "rust-storage-bench command runs", {
                    "status": status.code(),
                    "args": args_str,
                })
            }
            Err(err) => {
                precept::expect_unreachable!("rust-storage-bench failed to finish", {
                    "err": err.to_string(),
                    "args": args_str,
                });
            }
        };

        Ok(())
    }
}
