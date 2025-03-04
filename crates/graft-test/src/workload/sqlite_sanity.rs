use culprit::{Result, ResultExt};
use graft_client::runtime::storage::volume_state::{SyncDirection, VolumeConfig, VolumeStatus};
use graft_core::VolumeId;
use graft_sqlite::vfs::GraftVfs;
use precept::expect_sometimes;
use rand::{Rng, seq::IndexedRandom};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sqlite_plugin::vfs::{RegisterOpts, register_static};
use std::{fmt::Debug, thread::sleep, time::Duration};
use tracing::field;

use crate::workload::recover_and_sync_volume;

use super::{Workload, WorkloadEnv, WorkloadErr};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteSanity {
    vids: Vec<VolumeId>,
    interval_ms: u64,
    initial_accounts: u64,
    vfs_name: String,

    #[serde(skip)]
    vid: Option<VolumeId>,
}

impl Workload for SqliteSanity {
    fn setup<R: rand::Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        // register graft vfs
        let vfs = GraftVfs::new(env.runtime.clone());
        register_static(&self.vfs_name, vfs, RegisterOpts { make_default: false })
            .expect("failed to register vfs");

        Ok(())
    }

    fn run<R: rand::Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        let interval = Duration::from_millis(self.interval_ms);

        // pick volume id randomly and store in self.vid to handle workload retries
        let vid = self
            .vid
            .get_or_insert_with(|| self.vids.choose(&mut env.rng).unwrap().clone());
        tracing::info!("SqliteSanity workload is using volume: {}", vid);

        let mut sqlite = Connection::open_with_flags_and_vfs(
            vid.pretty(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &self.vfs_name,
        )?;

        let handle = env
            .runtime
            .open_volume(vid, VolumeConfig::new(SyncDirection::Both))
            .or_into_ctx()?;

        let status = handle.status().or_into_ctx()?;
        expect_sometimes!(
            status != VolumeStatus::Ok,
            "volume is not ok when workload starts",
            { "cid": env.cid, "vid": vid, "status": status }
        );

        // ensure the volume is recovered and synced with the server
        recover_and_sync_volume(&handle).or_into_ctx()?;

        // if the snapshot is empty attempt to initialize the schema and initial accounts
        let txn = sqlite.transaction()?;
        if get_snapshot(&txn)?.is_none() {
            txn.execute(SQL_ACCOUNTS_TABLE, [])?;

            let balance_per_account = TOTAL_BALANCE / self.initial_accounts;
            assert_eq!(
                TOTAL_BALANCE % self.initial_accounts,
                0,
                "total balance must be some multiple of initial accounts"
            );

            tracing::info!(
                "initializing {} accounts with balance {}",
                self.initial_accounts,
                balance_per_account
            );

            for _ in 0..self.initial_accounts {
                txn.execute(
                    "INSERT INTO accounts (balance) VALUES (?)",
                    [balance_per_account],
                )?;
            }
            txn.commit()?;
        } else {
            txn.rollback()?;
        }

        while env.ticker.tick() {
            // check the volume status to see if we need to reset
            let status = handle.status().or_into_ctx()?;
            if status != VolumeStatus::Ok {
                let span = tracing::info_span!("reset_volume", ?status, vid=?handle.vid(), result=field::Empty).entered();
                precept::expect_always_or_unreachable!(
                    status != VolumeStatus::InterruptedPush,
                    "volume needs reset after workload start",
                    { "cid": env.cid, "vid": vid, "status": status }
                );
                // reset the volume to the latest remote snapshot
                handle.reset_to_remote().or_into_ctx()?;
                span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
            }

            let txn = sqlite.transaction()?;
            let snapshot = get_snapshot(&txn)?;
            let action = Actions::random(&mut env.rng);

            let span = tracing::info_span!("running action", ?vid, ?action, ?snapshot).entered();
            action
                .run(&vid, env, &txn)
                .or_into_culprit(format!("txn snapshot: {snapshot:?}"))?;

            // run check balance after every action to help debug antithesis bug
            Actions::CheckBalance.run(&vid, env, &txn).or_into_ctx()?;

            txn.commit()?;
            drop(span);

            sleep(interval);
        }

        // run a final set of checks
        let txn = sqlite.transaction()?;
        let snapshot = get_snapshot(&txn)?;
        let _span = tracing::info_span!("performing final checks", ?vid, ?snapshot).entered();
        let final_actions = [
            Actions::CheckBalance,
            Actions::IntegrityCheck,
            Actions::CountAccounts,
        ];
        for action in &final_actions {
            let _span = tracing::info_span!("running action", ?action).entered();
            action.run(&vid, env, &txn).or_into_ctx()?;
        }
        txn.commit()?;

        Ok(())
    }
}

// The sum of all account balances must always equal this number
const TOTAL_BALANCE: u64 = 1_000_000;

const SQL_ACCOUNTS_TABLE: &str = r#"
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    balance INTEGER NOT NULL,
    CHECK (balance >= 0)
)
"#;

#[derive(Debug, Clone, Copy)]
enum Actions {
    // Attempts to create an account from a portion of another account's balance
    CreateAccount,

    // Attempts to transfer a random amount between two accounts
    Transfer,

    // Verifies that SUM(balance) equals TOTAL_BALANCE
    CheckBalance,

    // Prints out the total number of accounts
    CountAccounts,

    // Verifies the integrity of the database
    IntegrityCheck,
}

impl Actions {
    fn random<R: Rng>(rng: &mut R) -> Self {
        let probabilities = [
            (Actions::CreateAccount, 8),
            (Actions::Transfer, 16),
            (Actions::CheckBalance, 4),
            (Actions::CountAccounts, 2),
            (Actions::IntegrityCheck, 1),
        ];

        let sum = probabilities.iter().map(|(_, p)| p).sum();
        let mut cumulative = 0;
        let rand_value = rng.random_range(0..sum);

        for (action, probability) in probabilities {
            cumulative += probability;
            if rand_value < cumulative {
                return action;
            }
        }

        unreachable!("bug in random action selection");
    }

    fn run<R: Rng>(
        self,
        vid: &VolumeId,
        env: &mut WorkloadEnv<R>,
        txn: &Transaction<'_>,
    ) -> Result<(), rusqlite::Error> {
        let max_account_id: u64 =
            txn.query_row("SELECT MAX(id) FROM accounts", [], |r| r.get(0))?;

        match self {
            Actions::CreateAccount => {
                let (source, balance) = find_nonzero_account(&mut env.rng, txn, max_account_id)?;
                let amount = env.rng.random_range(1..=balance);
                txn.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                    [amount, source],
                )?;
                txn.execute("INSERT INTO accounts (balance) VALUES (?)", [amount])?;
            }
            Actions::Transfer => {
                let (source, balance) = find_nonzero_account(&mut env.rng, txn, max_account_id)?;
                let target = env.rng.random_range(1..=max_account_id);
                if balance > 0 && account_exists(txn, target)? {
                    let amount = env.rng.random_range(1..=balance);
                    txn.execute(
                        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                        [amount, source],
                    )?;
                    txn.execute(
                        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                        [amount, target],
                    )?;
                }
            }
            Actions::CheckBalance => {
                let total_balance: u64 =
                    txn.query_row("SELECT SUM(balance) FROM accounts", [], |r| r.get(0))?;
                precept::expect_always_or_unreachable!(
                    total_balance == TOTAL_BALANCE,
                    "total balance does not match expected value",
                    { "vid": vid, "cid": env.cid, "total_balance": total_balance, "expected": TOTAL_BALANCE }
                );
            }
            Actions::CountAccounts => {
                let count: u64 =
                    txn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
                tracing::info!("total accounts: {}", count);
                precept::expect_always_or_unreachable!(
                    count > 0,
                    "there should never be zero accounts",
                    { "vid": vid, "cid": env.cid, "count": count }
                );
            }
            Actions::IntegrityCheck => {
                let mut results: Vec<String> = vec![];
                txn.pragma_query(None, "integrity_check", |r| Ok(results.push(r.get(0)?)))?;
                precept::expect_always_or_unreachable!(
                    results == ["ok"],
                    "sqlite database is corrupt",
                    { "vid": vid, "cid": env.cid, "results": results }
                );
            }
        }

        Ok(())
    }
}

fn account_exists(txn: &Transaction<'_>, id: u64) -> rusqlite::Result<bool> {
    txn.query_row("SELECT 1 FROM accounts WHERE id = ?", [id], |_| Ok(()))
        .optional()
        .map(|r| r.is_some())
}

fn find_nonzero_account<R: Rng>(
    rng: &mut R,
    txn: &Transaction<'_>,
    max_account_id: u64,
) -> rusqlite::Result<(u64, u64)> {
    // find the first non-zero balance account starting from a random account id
    let start = rng.random_range(1..=max_account_id);
    let (id, balance) = first_nonzero_account_starting_at(txn, start)?;
    assert!(balance > 0, "balance should be greater than zero");
    return Ok((id, balance));
}

// start scanning for nonzero account at the provided id, wrapping around if
// needed
const FIRST_NONZERO_ACCOUNT_SQL: &str = r#"
SELECT id, balance FROM accounts WHERE balance > 0 and id >= ?
UNION ALL
SELECT id, balance FROM accounts WHERE balance > 0
"#;

fn first_nonzero_account_starting_at(
    txn: &Transaction<'_>,
    start: u64,
) -> rusqlite::Result<(u64, u64)> {
    txn.query_row(FIRST_NONZERO_ACCOUNT_SQL, [start], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
}

fn get_snapshot(txn: &Transaction<'_>) -> rusqlite::Result<Option<String>> {
    txn.pragma_query_value(None, "graft_snapshot", |row| row.get(0))
        .optional()
}
