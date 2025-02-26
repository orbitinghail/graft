use std::{
    env,
    ffi::c_char,
    fs::File,
    io::{Error, ErrorKind, Write},
    path::{Path, PathBuf},
    process,
};

use super::{Dispatch, Event};
use crate::catalog::{CatalogEntry, Expectation};

use libloading::Library;
use serde::Serialize;
use serde_json::json;

pub enum AntithesisDispatch {
    Voidstar(LibVoidstarHandler),
    File(FileHandler),
}

impl AntithesisDispatch {
    pub fn try_load() -> Option<Self> {
        if let Ok(handler) = LibVoidstarHandler::try_load() {
            let dispatcher = Self::Voidstar(handler);
            dispatcher.emit_json(sdk_info());
            return Some(dispatcher);
        }
        if let Ok(handler) = FileHandler::try_load() {
            return Some(Self::File(handler));
        }
        None
    }

    pub fn try_load_boxed() -> Option<Box<dyn Dispatch>> {
        Self::try_load().map(|d| Box::new(d) as Box<dyn Dispatch>)
    }

    fn emit_json(&self, value: serde_json::Value) {
        match serde_json::to_string(&value) {
            Ok(json) => match self {
                Self::Voidstar(handler) => handler.output_json(&json),
                Self::File(handler) => {
                    if let Err(e) = handler.output_json(&json) {
                        eprintln!("Failed to output json to file: {e}");
                    }
                }
            },
            Err(e) => eprintln!("Failed to serialize json: {e}"),
        }
    }
}

impl Dispatch for AntithesisDispatch {
    fn emit(&self, event: Event) {
        match event {
            Event::RegisterEntry(entry) => {
                let info = AssertionInfo::new(entry, false, false, json!(null));
                let value = json!({ "antithesis_assert": info });
                self.emit_json(value)
            }
            Event::EmitEntry { entry, condition, details } => {
                let info = AssertionInfo::new(entry, true, condition, details);
                let value = json!({ "antithesis_assert": info });
                self.emit_json(value)
            }
            Event::SetupComplete { details } => self.emit_json(json!({
                "antithesis_setup": {
                    "status": "complete",
                    "details": details,
                }
            })),
            Event::Custom { name, value } => self.emit_json(json!({ name: value })),
            Event::Fault { name, details } => {
                self.emit_json(json!({
                    "precept_fault": { "name": name, "details": details }
                }));

                // it would be nice if we could ask antithesis to crash us,
                // but for now we exit 0 to simulate immediate power loss
                // while not triggering the "workload commands always exit 0"
                // antithesis expectation
                process::exit(0)
            }
        }
    }

    #[inline]
    fn random(&self) -> u64 {
        match self {
            Self::Voidstar(handler) => handler.random(),
            Self::File(handler) => handler.random(),
        }
    }
}

#[derive(Serialize)]
struct AssertionLocation<'a> {
    class: &'a str,
    function: &'a str,
    file: &'a str,
    begin_line: u32,
    begin_column: u32,
}

#[derive(Serialize)]
#[serde(rename_all(serialize = "lowercase"))]
enum AssertType {
    Always,
    Sometimes,
    Reachability,
}

#[derive(Serialize)]
struct AssertionInfo<'a> {
    assert_type: AssertType,
    display_type: &'a str,
    condition: bool,
    id: &'a str,
    message: &'a str,
    location: AssertionLocation<'a>,
    hit: bool,
    must_hit: bool,
    details: serde_json::Value,
}

impl<'a> AssertionInfo<'a> {
    fn new(
        entry: &'a CatalogEntry,
        hit: bool,
        condition: bool,
        details: serde_json::Value,
    ) -> Self {
        let location = entry.location();

        let (must_hit, assert_type, display_type) = match entry.expectation() {
            Expectation::Always => (true, AssertType::Always, "Always"),
            Expectation::AlwaysOrUnreachable => (false, AssertType::Always, "AlwaysOrUnreachable"),
            Expectation::Sometimes => (true, AssertType::Sometimes, "Sometimes"),
            Expectation::Reachable => (true, AssertType::Reachability, "Reachable"),
            Expectation::Unreachable => (false, AssertType::Reachability, "Unreachable"),
        };

        Self {
            assert_type,
            display_type,
            condition,
            id: entry.property(),
            message: entry.property(),
            location: AssertionLocation {
                class: entry.module(),
                function: entry.function(),
                file: location.file(),
                begin_line: location.line(),
                begin_column: location.column(),
            },
            hit,
            must_hit,
            details,
        }
    }
}

pub struct LibVoidstarHandler {
    // Not used directly but exists to ensure the library is loaded
    // and all the following function pointers points to valid memory.
    _lib: Library,
    // SAFETY: The memory pointed by `s` must be valid up to `l` bytes.
    fuzz_json_data: unsafe fn(s: *const c_char, l: usize),
    fuzz_get_random: fn() -> u64,
    fuzz_flush: fn(),
}

impl LibVoidstarHandler {
    fn try_load() -> Result<Self, libloading::Error> {
        // SAFETY:
        // - The `libvoidstar` library must not have initalization procedures.
        // - The `libvoidstar` library must export symbols with the expected type signatures.
        unsafe {
            let lib = Library::new("/usr/lib/libvoidstar.so")?;
            let fuzz_json_data = *lib.get(b"fuzz_json_data\0")?;
            let fuzz_get_random = *lib.get(b"fuzz_get_random\0")?;
            let fuzz_flush = *lib.get(b"fuzz_flush\0")?;
            Ok(Self {
                _lib: lib,
                fuzz_json_data,
                fuzz_get_random,
                fuzz_flush,
            })
        }
    }

    fn output_json(&self, value: &str) {
        let value_ptr = value.as_ptr() as *const c_char;
        let value_len = value.len();
        // SAFETY:
        // - The `fuzz_json_data` function must not hold onto the pointer after it returns
        unsafe {
            (self.fuzz_json_data)(value_ptr, value_len);
            (self.fuzz_flush)();
        }
    }

    #[inline]
    fn random(&self) -> u64 {
        (self.fuzz_get_random)()
    }
}

pub struct FileHandler {
    file: File,
}

impl FileHandler {
    fn try_load() -> Result<Self, Error> {
        const ENV_LOCAL_OUTPUT: &str = "ANTITHESIS_SDK_LOCAL_OUTPUT";
        const ENV_OUTPUT_DIR: &str = "ANTITHESIS_OUTPUT_DIR";

        let local_output: Option<PathBuf> = env::var(ENV_LOCAL_OUTPUT).ok().map(|s| s.into());
        let sdk_output: Option<PathBuf> = env::var(ENV_OUTPUT_DIR)
            .ok()
            .map(|s| Path::new(&s).join("sdk.jsonl"));

        if let Some(filename) = local_output.or(sdk_output) {
            return Ok(Self { file: File::create(&filename)? });
        }
        Err(Error::new(
            ErrorKind::NotFound,
            format!(
                "No output file specified. Set either {ENV_LOCAL_OUTPUT} or {ENV_OUTPUT_DIR}"
            ),
        ))
    }

    fn output_json(&self, value: &str) -> Result<(), Error> {
        debug_assert!(
            !value.contains("\n"),
            "output must be a valid jsonl message; got `{value}`",
        );
        let mut writer = &self.file;
        writer.write_all(value.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    #[inline]
    fn random(&self) -> u64 {
        rand::random()
    }
}

fn sdk_info() -> serde_json::Value {
    json!({
        "antithesis_sdk": {
            "language": {
                "name": "Rust",
                "version": rustc_version_runtime::version().to_string(),
            },
            "sdk_version": env!("CARGO_PKG_VERSION"),
            "protocol_version": "1.1.0",
        }
    })
}
