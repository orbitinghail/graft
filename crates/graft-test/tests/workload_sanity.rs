use graft::core::LogId;
use graft_test::GraftTestRuntime;
use graft_test::workload::{Env, bank_setup, bank_tx, bank_validate};

#[test]
fn test_bank_workload() {
    graft_test::setup_precept_and_disable_faults();

    let mut runtime = GraftTestRuntime::with_memory_remote();

    // setup the main tag
    let log = LogId::random();
    let volume = runtime.volume_open(None, None, Some(log.clone())).unwrap();
    runtime.tag_replace("main", volume.vid.clone()).unwrap();

    // create test env
    let sqlite = runtime.open_sqlite("main", None).into();
    let rng = rand::rng();
    let mut env = Env {
        rng,
        runtime: runtime.clone(),
        vid: volume.vid,
        log,
        sqlite,
    };

    // Run bank_setup -> bank_tx -> bank_validate
    bank_setup(&mut env).unwrap();
    bank_tx(&mut env).unwrap();
    bank_validate(&mut env).unwrap();

    runtime.shutdown().unwrap();
}
