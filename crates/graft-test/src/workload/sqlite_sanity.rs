use culprit::{Result, ResultExt};
use graft_client::runtime::storage::volume_state::{SyncDirection, VolumeConfig, VolumeStatus};
use graft_core::{ClientId, VolumeId};
use graft_sqlite::vfs::GraftVfs;
use precept::expect_sometimes;
use rand::{Rng, seq::IndexedRandom};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, config::DbConfig, params};
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

        sqlite.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)?;

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
        recover_and_sync_volume(&env.cid, &handle).or_into_ctx()?;

        // if the snapshot is empty attempt to initialize the schema and initial accounts
        let txn = sqlite.transaction()?;
        if get_snapshot(&txn)?.is_none() {
            txn.execute(SQL_ACCOUNTS_TABLE, [])?;
            txn.execute(SQL_LEDGER_TABLE, [])?;
            txn.execute(SQL_LEDGER_INDEX, [])?;

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
                let account_id = txn.query_row(
                    "INSERT INTO accounts (balance) VALUES (?) RETURNING id",
                    [balance_per_account],
                    |r| r.get(0),
                )?;
                write_to_ledger(&txn, account_id, balance_per_account as i64, &env.cid)?;
            }
            txn.commit()?;
        } else {
            txn.rollback()?;
        }

        // perform a balance and corruption check before starting the workload
        let txn = sqlite.transaction()?;
        check_db(env, vid, &txn)?;
        txn.commit()?;

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
                drop(span);

                // perform a balance and corruption check after resetting the volume
                let txn = sqlite.transaction()?;
                check_db(env, vid, &txn)?;
                txn.commit()?;
            }

            let txn = sqlite.transaction()?;
            let snapshot = get_snapshot(&txn)?;
            let action = Actions::random(&mut env.rng);
            let span = tracing::info_span!("running action", ?vid, ?action, ?snapshot).entered();
            action
                .run(vid, env, &txn)
                .or_into_culprit(format!("txn snapshot: {snapshot:?}"))?;
            txn.commit()?;
            drop(span);

            sleep(interval);
        }

        // run a final set of checks
        let txn = sqlite.transaction()?;
        check_db(env, vid, &txn)?;
        txn.commit()?;

        Ok(())
    }
}

// The sum of all account balances must always equal this number
const TOTAL_BALANCE: u64 = 1_000_000;

const SQL_ACCOUNTS_TABLE: &str = r#"
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY NOT NULL,
    balance INTEGER NOT NULL,
    CHECK (balance >= 0)
) STRICT
"#;

const SQL_LEDGER_TABLE: &str = r#"
CREATE TABLE ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    account_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    snapshot TEXT NOT NULL,
    cid TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
) STRICT
"#;

const SQL_LEDGER_INDEX: &str = r#"
CREATE INDEX ledger_account_id ON ledger (account_id)
"#;

#[derive(Debug, Clone, Copy)]
enum Actions {
    // Attempts to create an account from a portion of another account's balance
    CreateAccount,

    // Attempts to transfer a random amount between two accounts
    Transfer,

    // Verifies that SUM(balance) equals TOTAL_BALANCE
    CheckBalance,

    // Verifies the integrity of the database
    IntegrityCheck,
}

impl Actions {
    fn random<R: Rng>(rng: &mut R) -> Self {
        let probabilities = [
            (Actions::CreateAccount, 8),
            (Actions::Transfer, 16),
            (Actions::CheckBalance, 4),
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
                let target = txn.query_row(
                    "INSERT INTO accounts (balance) VALUES (0) RETURNING id",
                    [],
                    |r| r.get(0),
                )?;
                transfer(txn, source, target, amount as i64, &env.cid)?;
            }
            Actions::Transfer => {
                let (source, balance) = find_nonzero_account(&mut env.rng, txn, max_account_id)?;
                let target = env.rng.random_range(1..=max_account_id);
                if balance > 0 && account_exists(txn, target)? {
                    let amount = env.rng.random_range(1..=balance);
                    transfer(txn, source, target, amount as i64, &env.cid)?;
                }
            }
            Actions::CheckBalance => {
                // verify that each account's balance matches the ledger
                let mut total_balance = 0;
                let mut stmt = txn.prepare("SELECT id, balance FROM accounts")?;
                let mut rows = stmt.query([])?;

                while let Some(row) = rows.next()? {
                    let account_id: u64 = row.get(0)?;
                    let balance: i64 = row.get(1)?;
                    total_balance += balance;

                    let ledger_balance: i64 = txn.query_row(
                        "SELECT SUM(amount) FROM ledger WHERE account_id = ?",
                        [account_id],
                        |r| r.get(0),
                    )?;

                    if balance != ledger_balance {
                        // account is out of sync with ledger, print full ledger to log
                        tracing::error!(
                            ?vid, cid = ?env.cid, account_id, balance, ledger_balance,
                            "account balance mismatch; printing ledger",
                        );

                        let mut stmt = txn
                            .prepare("SELECT amount, snapshot, cid FROM ledger WHERE account_id = ? ORDER BY id ASC")?;
                        let mut rows = stmt.query([account_id])?;
                        while let Some(row) = rows.next()? {
                            let amount: i64 = row.get(0)?;
                            let snapshot: String = row.get(1)?;
                            let cid: String = row.get(2)?;
                            tracing::error!("{cid} {amount:<5} {snapshot}",);
                        }
                    }

                    precept::expect_always_or_unreachable!(
                        balance == ledger_balance,
                        "account balance must match ledger balance",
                        { "vid": vid, "cid": env.cid, "account_id": account_id, "balance": balance, "ledger_balance": ledger_balance }
                    );
                }

                precept::expect_always_or_unreachable!(
                    total_balance == TOTAL_BALANCE as i64,
                    "total balance does not match expected value",
                    { "vid": vid, "cid": env.cid, "total_balance": total_balance, "expected": TOTAL_BALANCE }
                );
            }
            Actions::IntegrityCheck => {
                let mut results: Vec<String> = vec![];
                txn.pragma_query(None, "integrity_check", |r| {
                    results.push(r.get(0)?);
                    Ok(())
                })?;
                precept::expect_always_or_unreachable!(
                    results == ["ok"],
                    "sqlite database is corrupt",
                    { "vid": vid, "cid": env.cid, "results": results }
                );
                let mut results: Vec<(String, String, String, String)> = vec![];
                txn.pragma_query(None, "foreign_key_check", |r| {
                    results.push((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?));
                    Ok(())
                })?;
                precept::expect_always_or_unreachable!(
                    results.is_empty(),
                    "sqlite database has invalid foreign key references",
                    { "vid": vid, "cid": env.cid, "results": results }
                );
            }
        }

        Ok(())
    }
}

fn check_db<R: rand::Rng>(
    env: &mut WorkloadEnv<R>,
    vid: &VolumeId,
    txn: &Transaction<'_>,
) -> Result<(), WorkloadErr> {
    let snapshot = get_snapshot(txn)?;
    let _span = tracing::info_span!("performing initial checks", ?vid, ?snapshot).entered();
    let checks = [Actions::CheckBalance, Actions::IntegrityCheck];
    for action in &checks {
        let _span = tracing::info_span!("running action", ?action).entered();
        action.run(vid, env, txn).or_into_ctx()?;
    }
    Ok(())
}

fn account_exists(txn: &Transaction<'_>, id: u64) -> rusqlite::Result<bool> {
    txn.query_row(
        "select exists(select * from accounts where id = ?)",
        [id],
        |row| row.get(0),
    )
}

// start scanning for nonzero account at the provided id, wrapping around if
// needed
const FIRST_NONZERO_ACCOUNT_SQL: &str = r#"
SELECT id, balance FROM accounts WHERE balance > 0 and id >= ?
UNION ALL
SELECT id, balance FROM accounts WHERE balance > 0
"#;

fn find_nonzero_account<R: Rng>(
    rng: &mut R,
    txn: &Transaction<'_>,
    max_account_id: u64,
) -> rusqlite::Result<(u64, u64)> {
    // find the first non-zero balance account starting from a random account id
    let start = rng.random_range(1..=max_account_id);
    let (id, balance) = txn.query_row(FIRST_NONZERO_ACCOUNT_SQL, [start], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?;
    assert!(balance > 0, "balance should be greater than zero");
    Ok((id, balance))
}

fn get_snapshot(txn: &Transaction<'_>) -> rusqlite::Result<Option<String>> {
    txn.pragma_query_value(None, "graft_snapshot", |row| row.get(0))
        .optional()
}

fn write_to_ledger(
    txn: &Transaction<'_>,
    account_id: u64,
    amount: i64,
    cid: &ClientId,
) -> rusqlite::Result<()> {
    let snapshot = get_snapshot(txn)?.unwrap_or("empty".to_string());
    txn.execute(
        "INSERT INTO ledger (account_id, amount, snapshot, cid) VALUES (?, ?, ?, ?)",
        params![account_id, amount, snapshot, cid.short()],
    )?;
    Ok(())
}

fn transfer(
    txn: &Transaction<'_>,
    source: u64,
    target: u64,
    amount: i64,
    cid: &ClientId,
) -> rusqlite::Result<()> {
    write_to_ledger(txn, source, -amount, cid)?;
    txn.execute(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        params![amount, source],
    )?;
    write_to_ledger(txn, target, amount, cid)?;
    txn.execute(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        params![amount, target],
    )?;
    Ok(())
}
