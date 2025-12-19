use graft::{
    GraftErr, LogicalErr,
    core::{LogId, PageCount, VolumeId},
    remote::RemoteErr,
    rt::runtime::Runtime,
};
use rand::Rng;
use rusqlite::Connection;

#[derive(Debug, thiserror::Error)]
pub enum WorkloadErr {
    #[error(transparent)]
    GraftErr(#[from] GraftErr),

    #[error(transparent)]
    RusqliteErr(#[from] rusqlite::Error),
}

impl WorkloadErr {
    pub fn should_retry(&self) -> bool {
        match self {
            WorkloadErr::GraftErr(GraftErr::Logical(
                LogicalErr::VolumeConcurrentWrite(_)
                | LogicalErr::VolumeNeedsRecovery(_)
                | LogicalErr::VolumeDiverged(_),
            )) => true,
            WorkloadErr::GraftErr(GraftErr::Remote(RemoteErr::ObjectStore(err))) => {
                err.is_temporary() || err.is_persistent()
            }
            WorkloadErr::RusqliteErr(rusqlite::Error::SqliteFailure(err, _)) => matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::SystemIoFailure
            ),
            _ => false,
        }
    }
}

pub struct Env<R> {
    pub cid: String,
    pub rng: R,
    pub runtime: Runtime,
    pub vid: VolumeId,
    pub log: LogId,
    pub sqlite: Connection,
}

const NUM_ACCOUNTS: usize = 100_000;
const INITIAL_BALANCE: u64 = 1_000;
const TOTAL_BALANCE: u64 = NUM_ACCOUNTS as u64 * INITIAL_BALANCE;

pub fn bank_setup<R: Rng>(env: &mut Env<R>) -> Result<(), WorkloadErr> {
    let Env { sqlite, runtime, vid, .. } = env;

    // disable fault injection during setup
    precept::fault::disable_all();

    tracing::info!("setting up bank workload with {} accounts", NUM_ACCOUNTS);

    // start a sql tx
    let tx = sqlite.transaction()?;

    tx.execute("DROP TABLE IF EXISTS accounts", [])?;

    // create an accounts table with an integer primary key and a balance
    tx.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL CHECK (balance >= 0)
        )",
        [],
    )?;

    // initialize the accounts table with NUM_ACCOUNTS each starting with INITIAL_BALANCE
    let mut stmt = tx.prepare("INSERT INTO accounts (id, balance) VALUES (?, ?)")?;
    for id in 0..NUM_ACCOUNTS {
        assert_eq!(1, stmt.execute([id as i64, INITIAL_BALANCE as i64])?);
    }
    drop(stmt);

    tx.commit()?;

    // run runtime.volume_push
    runtime.volume_push(vid.clone())?;

    Ok(())
}

/// pull the remote log if the local volume is empty
pub fn pull_if_empty<R: Rng>(env: &Env<R>) -> Result<(), WorkloadErr> {
    let snapshot = env.runtime.volume_snapshot(&env.vid)?;
    if snapshot.is_empty() || env.runtime.snapshot_pages(&snapshot)? == PageCount::ZERO {
        precept::expect_reachable!("pull_if_empty triggers");
        Ok(env.runtime.volume_pull(env.vid.clone())?)
    } else {
        precept::expect_reachable!("pull_if_empty doesn't trigger");
        Ok(())
    }
}

pub fn bank_tx<R: Rng>(env: &mut Env<R>) -> Result<(), WorkloadErr> {
    pull_if_empty(&env)?;

    let rng = &mut env.rng;
    let sqlite = &mut env.sqlite;
    let runtime = &env.runtime;
    let vid = &env.vid;

    // randomly choose a number of transactions to make
    let total_txns = rng.random_range(1..=100);
    let mut valid_txns = 0;

    let status = env.runtime.volume_status(&env.vid)?;
    let snapshot = env.runtime.volume_snapshot(&env.vid)?;
    tracing::info!(%status, ?snapshot, "performing {} bank transactions", total_txns);

    while valid_txns < total_txns {
        // randomly pick two account ids (they are between 0 and NUM_ACCOUNTS)
        let id_a = rng.random_range(0..NUM_ACCOUNTS) as i64;
        let id_b = rng.random_range(0..NUM_ACCOUNTS) as i64;
        if id_a == id_b {
            continue;
        }

        // start a sql tx
        let tx = sqlite.transaction()?;

        // check both account balances
        let balance_a: i64 =
            tx.query_row("SELECT balance FROM accounts WHERE id = ?", [id_a], |row| {
                row.get(0)
            })?;
        let balance_b: i64 =
            tx.query_row("SELECT balance FROM accounts WHERE id = ?", [id_b], |row| {
                row.get(0)
            })?;

        // we always transfer from the larger account to the smaller account
        let (from_id, to_id, from_balance) = if balance_a > balance_b {
            (id_a, id_b, balance_a)
        } else {
            (id_b, id_a, balance_b)
        };

        // transfer up to $50 or the entire balance
        let transfer_amount = 50.min(from_balance);

        if transfer_amount > 0 {
            valid_txns += 1;

            assert_eq!(
                1,
                tx.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                    [transfer_amount, from_id],
                )?
            );
            assert_eq!(
                1,
                tx.execute(
                    "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                    [transfer_amount, to_id],
                )?
            );

            tracing::info!(
                "transferring ${} from account {} to account {}",
                transfer_amount,
                from_id,
                to_id
            );
        }

        // commit the tx
        tx.commit()?;
    }

    let status = runtime.volume_status(vid)?;
    let changes = status.local_status.changes();
    precept::expect_always_or_unreachable!(
        changes.is_some(),
        "bank tx always pushes some valid txns"
    );

    // attempt to push
    runtime.volume_push(vid.clone())?;

    Ok(())
}

pub fn bank_validate<R: Rng>(env: &mut Env<R>) -> Result<(), WorkloadErr> {
    tracing::info!("validating bank workload");

    // disable fault injection during validation
    precept::fault::disable_all();

    pull_if_empty(&env)?;

    // pull the database
    env.runtime.volume_pull(env.vid.clone())?;

    let status = env.runtime.volume_status(&env.vid)?;
    let snapshot = env.runtime.volume_snapshot(&env.vid)?;

    // hydrate and checksum the database
    env.runtime.snapshot_hydrate(snapshot.clone())?;
    let checksum = env.runtime.snapshot_checksum(&snapshot)?;

    tracing::info!(%status, ?snapshot, %checksum, "volume state");

    // verify that the total balance (sum(balance)) is equal to TOTAL_BALANCE
    let total: i64 = env
        .sqlite
        .query_row("SELECT SUM(balance) FROM accounts", [], |row| row.get(0))?;

    precept::expect_always_or_unreachable!(
        total as u64 == TOTAL_BALANCE,
        "validate: bank is balanced",
        { "expected": TOTAL_BALANCE, "actual": total, "cid": env.cid }
    );

    // run SQLite integrity check
    let mut results: Vec<String> = vec![];
    env.sqlite.pragma_query(None, "integrity_check", |r| {
        results.push(r.get(0)?);
        Ok(())
    })?;
    precept::expect_always_or_unreachable!(
        results == ["ok"],
        "validate: sqlite database is not corrupt",
        { "vid": env.vid, "results": results }
    );

    Ok(())
}
