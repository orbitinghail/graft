use std::{
    panic::panic_any,
    sync::atomic::{AtomicBool, Ordering},
};

/// If `FAULT_MODE` is false, then `fault_crash` will exit the process with code 0
/// If `FAULT_MODE` is true, then `fault_crash` will panic
static FAULT_MODE: AtomicBool = AtomicBool::new(false);

pub fn set_crash_mode(panic: bool) {
    FAULT_MODE.store(panic, Ordering::Release);
}

pub struct PowerlossPanic;

pub fn fault_crash() {
    if FAULT_MODE.load(Ordering::Acquire) {
        panic_any(PowerlossPanic)
    } else {
        std::process::exit(0);
    }
}
