use culprit::{Result, ResultExt};
use graft_client::runtime::storage::volume_state::{SyncDirection, VolumeConfig, VolumeStatus};
use graft_core::VolumeId;
use graft_sqlite::vfs::GraftVfs;
use rand::{Rng, seq::IndexedRandom};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sqlite_plugin::vfs::{RegisterOpts, register_static};
use std::{
    fmt::Debug,
    thread::{self, sleep},
    time::Duration,
};
use tracing::field;

use super::{Workload, WorkloadEnv, WorkloadErr};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteSanity {
    vids: Vec<VolumeId>,
    interval_ms: u64,
    initial_accounts: u64,

    #[serde(skip)]
    vid: Option<VolumeId>,
}

fn thread_vfs_name() -> String {
    let thread_name = thread::current().name().unwrap().to_owned();
    format!("{thread_name}-graft")
}

impl Workload for SqliteSanity {
    fn setup<R: rand::Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        // pick volume id randomly and store in self.vid
        let vid = self
            .vid
            .get_or_insert_with(|| self.vids.choose(&mut env.rng).unwrap().clone());
        tracing::info!("SqliteNode workload is using volume: {}", vid);

        // register graft vfs
        let vfs = GraftVfs::new(env.runtime.clone());
        register_static(
            &thread_vfs_name(),
            vfs,
            RegisterOpts { make_default: false },
        )
        .expect("failed to register vfs");

        let mut sqlite = Connection::open_with_flags_and_vfs(
            vid.pretty(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &thread_vfs_name(),
        )?;

        // create schema and initial accounts
        let txn = sqlite.transaction()?;

        txn.execute(SQL_ACCOUNTS_TABLE, [])?;
        txn.execute("INSERT INTO accounts (balance) VALUES (?)", [TOTAL_BALANCE])?;

        for _ in 0..self.initial_accounts {
            Actions::CreateAccount
                .run(&mut env.rng, &txn)
                .or_into_ctx()?;
        }
        txn.commit()?;

        Ok(())
    }

    fn run<R: rand::Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        let interval = Duration::from_millis(self.interval_ms);
        let vid = self.vid.as_ref().expect("volume id not set");
        let mut sqlite = Connection::open_with_flags_and_vfs(
            vid.pretty(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &thread_vfs_name(),
        )?;

        let handle = env
            .runtime
            .open_volume(vid, VolumeConfig::new(SyncDirection::Both))
            .or_into_ctx()?;

        while env.ticker.tick() {
            // check the volume status to see if we need to reset
            let status = handle.status().or_into_ctx()?;
            if status != VolumeStatus::Ok {
                let span = tracing::info_span!("reset_volume", ?status, vid=?handle.vid(), result=field::Empty).entered();
                precept::expect_always_or_unreachable!(
                    status != VolumeStatus::InterruptedPush,
                    "volume needs reset after workload start",
                    { "cid": env.cid, "vid": handle.vid(), "status": status }
                );
                // reset the volume to the latest remote snapshot
                handle.reset_to_remote().or_into_ctx()?;
                span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
            }

            let txn = sqlite.transaction()?;
            let snapshot =
                txn.pragma_query_value(None, "graft_snapshot", |row| row.get::<_, String>(0))?;
            let action = Actions::random(&mut env.rng);

            let span = tracing::info_span!("running action", ?vid, ?action, ?snapshot).entered();
            action
                .run(&mut env.rng, &txn)
                .map_err(|err| err.with_note(format!("current snapshot: {snapshot}")))
                .or_into_ctx()?;
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
CREATE TABLE IF NOT EXISTS accounts (
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

    fn run<R: Rng>(self, rng: &mut R, txn: &Transaction<'_>) -> Result<(), rusqlite::Error> {
        let max_account_id: u64 =
            txn.query_row("SELECT MAX(id) FROM accounts", [], |r| r.get(0))?;

        match self {
            Actions::CreateAccount => {
                let (source, balance) = find_nonzero_account(rng, txn, max_account_id)?;
                let amount = rng.random_range(1..=balance);
                txn.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                    [amount, source],
                )?;
                txn.execute("INSERT INTO accounts (balance) VALUES (?)", [amount])?;
            }
            Actions::Transfer => {
                let (source, balance) = find_nonzero_account(rng, txn, max_account_id)?;
                let target = rng.random_range(1..=max_account_id);
                if balance > 0 && account_exists(txn, target)? {
                    let amount = rng.random_range(1..=balance);
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
                    { "total_balance": total_balance, "expected": TOTAL_BALANCE }
                );
            }
            Actions::CountAccounts => {
                let count: u64 =
                    txn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
                tracing::info!("total accounts: {}", count);
                precept::expect_always_or_unreachable!(
                    count > 0,
                    "there should never be zero accounts",
                    { "count": count }
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

fn account_balance(txn: &Transaction<'_>, id: u64) -> rusqlite::Result<u64> {
    txn.query_row("SELECT balance FROM accounts WHERE id = ?", [id], |r| {
        r.get(0)
    })
    .optional()
    .map(|r| r.unwrap_or(0))
}

/// returns the account id and balance of the first nonempty account found through random guessing
fn find_nonzero_account<R: Rng>(
    rng: &mut R,
    txn: &Transaction<'_>,
    max_account_id: u64,
) -> rusqlite::Result<(u64, u64)> {
    loop {
        let id = rng.random_range(1..=max_account_id);
        let balance = account_balance(txn, id)?;
        if balance > 0 {
            return Ok((id, balance));
        }
    }
}
