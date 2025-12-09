use graft::{
    GraftErr, LogicalErr,
    core::{LogId, VolumeId},
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

pub struct Env<R> {
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

    tracing::info!("setting up bank workload with {} accounts", NUM_ACCOUNTS);

    // start a sql tx
    let tx = sqlite.transaction()?;

    tx.execute("DROP TABLE if exists accounts", [])?;

    // create an accounts table with an integer primary key and a balance
    tx.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
        [],
    )?;

    // initialize the accounts table with NUM_ACCOUNTS each starting with INITIAL_BALANCE
    let mut stmt = tx.prepare("INSERT OR IGNORE INTO accounts (id, balance) VALUES (?, ?)")?;
    for id in 0..NUM_ACCOUNTS {
        stmt.execute([id as i64, INITIAL_BALANCE as i64])?;
    }
    drop(stmt);

    tx.commit()?;

    // run runtime.volume_push
    runtime.volume_push(vid.clone())?;

    Ok(())
}

fn run_bank_transactions(
    rng: &mut impl Rng,
    sqlite: &mut Connection,
    runtime: &Runtime,
    vid: &VolumeId,
) -> Result<(), WorkloadErr> {
    // randomly choose a number of transactions to make
    let num_transactions = rng.random_range(1..=100);

    tracing::info!("performing {} bank transactions", num_transactions);

    for _ in 0..num_transactions {
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

        // send half of the balance of the larger account to the smaller account
        let (from_id, to_id, transfer_amount) = if balance_a > balance_b {
            (id_a, id_b, balance_a / 2)
        } else {
            (id_b, id_a, balance_b / 2)
        };

        if transfer_amount > 0 {
            tx.execute(
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                [transfer_amount, from_id],
            )?;
            tx.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                [transfer_amount, to_id],
            )?;
        }

        // commit the tx
        tx.commit()?;
    }

    // attempt to push
    runtime.volume_push(vid.clone())?;

    Ok(())
}

pub fn bank_tx<R: Rng>(env: &mut Env<R>) -> Result<(), WorkloadErr> {
    loop {
        match run_bank_transactions(&mut env.rng, &mut env.sqlite, &env.runtime, &env.vid) {
            Ok(()) => return Ok(()),
            Err(WorkloadErr::GraftErr(GraftErr::Logical(LogicalErr::VolumeDiverged(_)))) => {
                tracing::warn!("volume diverged, performing recovery and retrying");

                // reopen the remote and update the tag
                let volume = env.runtime.volume_open(None, None, Some(env.log.clone()))?;
                env.runtime.tag_replace("main", volume.vid.clone())?;
                env.vid = volume.vid;

                // make sure we are up to date with the remote
                env.runtime.volume_pull(env.vid.clone())?;

                // reopen sqlite connection with new volume
                env.sqlite = Connection::open("main")?;
            }
            err => return err,
        }
    }
}

pub fn bank_validate<R: Rng>(env: &mut Env<R>) -> Result<(), WorkloadErr> {
    tracing::info!("validating bank workload");

    // pull the database
    env.runtime.volume_pull(env.vid.clone())?;

    // verify that the total balance (sum(balance)) is equal to TOTAL_BALANCE
    let total: i64 = env
        .sqlite
        .query_row("SELECT SUM(balance) FROM accounts", [], |row| row.get(0))?;

    assert_eq!(
        total as u64, TOTAL_BALANCE,
        "total balance mismatch: expected {}, got {}",
        TOTAL_BALANCE, total
    );

    Ok(())
}
