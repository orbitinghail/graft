extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=sqlite3/sqlite3.h");
    println!("cargo:rerun-if-changed=sqlite3/sqlite3ext.h");

    let vars = bindgen::Builder::default()
        .header("sqlite3/sqlite3ext.h")
        .allowlist_item("SQLITE_.*")
        .use_core()
        .default_macro_constant_type(bindgen::MacroTypeVariation::Signed)
        .generate()
        .expect("Unable to generate bindings");

    let bindings = bindgen::Builder::default()
        .header("sqlite3/sqlite3ext.h")
        .blocklist_item("SQLITE_.*")
        .use_core()
        .default_macro_constant_type(bindgen::MacroTypeVariation::Signed)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be defined"));
    vars.write_to_file(out_path.join("vars.rs"))
        .expect("Couldn't write vars!");
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
