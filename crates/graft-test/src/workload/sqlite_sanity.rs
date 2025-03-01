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
    fn module_path(&self) -> &'static str {
        module_path!()
    }

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
            { "cid": env.cid, "vid": vid }
        );

        // ensure the volume is recovered and synced with the server
        recover_and_sync_volume(&handle).or_into_ctx()?;

        // if the snapshot is empty attempt to initialize the schema and initial accounts
        let txn = sqlite.transaction()?;
        if get_snapshot(&txn)?.is_none() {
            txn.execute(SQL_ACCOUNTS_TABLE, [])?;
            txn.execute("INSERT INTO accounts (balance) VALUES (?)", [TOTAL_BALANCE])?;

            for _ in 0..self.initial_accounts {
                Actions::CreateAccount.run(env, &txn).or_into_ctx()?;
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
            if let Err(err) = action.run(env, &txn) {
                if matches!(
                    err.ctx().sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy)
                ) {
                    precept::expect_reachable!(
                        "database concurrently modified by sync",
                        { "cid": env.cid, "vid": vid, "snapshot": snapshot }
                    );
                    tracing::info!("database concurrently modified by sync");
                    txn.rollback()?;
                    continue;
                }
                return Err(err
                    .map_ctx(WorkloadErr::from)
                    .with_note(format!("txn snapshot: {snapshot:?}")));
            }
            txn.commit()?;
            drop(span);

            sleep(interval);
        }

        Ok(())
    }
}

// The sum of all account balances must always equal this number
const TOTAL_BALANCE: u64 = 1_000_000;

const SQL_ACCOUNTS_TABLE: &str = r#"
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
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
}

impl Actions {
    fn random<R: Rng>(rng: &mut R) -> Self {
        let probabilities = [
            (Actions::CreateAccount, 8),
            (Actions::Transfer, 16),
            (Actions::CheckBalance, 4),
            (Actions::CountAccounts, 1),
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

        unreachable!("random value should always be less than 1.0");
    }

    fn run<R: Rng>(
        self,
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
                    { "cid": env.cid, "total_balance": total_balance, "expected": TOTAL_BALANCE }
                );
            }
            Actions::CountAccounts => {
                let count: u64 =
                    txn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
                tracing::info!("total accounts: {}", count);
                precept::expect_always_or_unreachable!(
                    count > 0,
                    "there should never be zero accounts",
                    { "cid": env.cid, "count": count }
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

/// returns the account id and balance of the first nonempty account found through random guessing
fn find_nonzero_account<R: Rng>(
    rng: &mut R,
    txn: &Transaction<'_>,
    max_account_id: u64,
) -> rusqlite::Result<(u64, u64)> {
    // find the first non-zero balance account starting from a random account id
    let start = rng.random_range(1..=max_account_id);
    if let Some((id, balance)) = first_nonzero_account_starting_at(txn, start)? {
        assert!(balance > 0, "balance should be greater than zero");
        return Ok((id, balance));
    }

    // fall back to scanning the whole table
    if let Some((id, balance)) = first_nonzero_account_starting_at(txn, 1)? {
        assert!(balance > 0, "balance should be greater than zero");
        return Ok((id, balance));
    }

    unreachable!("unable to find any nonzero accounts")
}

fn first_nonzero_account_starting_at(
    txn: &Transaction<'_>,
    start: u64,
) -> rusqlite::Result<Option<(u64, u64)>> {
    txn.query_row(
        "SELECT id, balance FROM accounts WHERE balance > 0 and id >= ?",
        [start],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
}

fn get_snapshot(txn: &Transaction<'_>) -> rusqlite::Result<Option<String>> {
    txn.pragma_query_value(None, "graft_snapshot", |row| row.get(0))
        .optional()
}
