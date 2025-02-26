use std::{
    panic::Location,
    sync::{
        atomic::{self, AtomicUsize},
        LazyLock,
    },
};

use crate::dispatch::{self, dispatcher, Event};

/// Catalog of all antithesis assertions provided
#[cfg(not(feature = "disabled"))]
#[linkme::distributed_slice]
pub static PRECEPT_CATALOG: [CatalogEntry];

#[cfg(feature = "disabled")]
pub static PRECEPT_CATALOG: [&CatalogEntry; 0] = [];

pub fn init_catalog() {
    let dispatch = dispatcher();
    for entry in PRECEPT_CATALOG {
        dispatch.emit(Event::RegisterEntry(entry));
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Expectation {
    Always,
    AlwaysOrUnreachable,
    Sometimes,
    Reachable,
    Unreachable,
}

impl Expectation {
    pub fn check(self, condition: bool) -> bool {
        use Expectation::*;

        match (self, condition) {
            (Always | AlwaysOrUnreachable, out) => out,
            (Sometimes | Reachable, _) => true,
            (Unreachable, _) => false,
        }
    }
}

#[derive(Debug)]
pub struct CatalogEntry {
    // the type of this expectation
    expectation: Expectation,
    // the name of the entry, also serves as it's id
    property: &'static str,
    // panic::Location::caller()
    location: &'static Location<'static>,
    // from module_path!()
    module: &'static str,
    // from function_name!()
    function: &'static LazyLock<&'static str>,

    // the number of times this entry has been encountered with a true condition
    pass_count: AtomicUsize,
    // the number of times this entry has been encountered with a false condition
    fail_count: AtomicUsize,
}

impl CatalogEntry {
    #[inline]
    pub const fn new(
        expectation: Expectation,
        property: &'static str,
        location: &'static Location<'static>,
        module: &'static str,
        function: &'static LazyLock<&'static str>,
    ) -> Self {
        Self {
            expectation,
            property,
            location,
            module,
            function,
            pass_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
        }
    }

    pub fn emit(&'static self, condition: bool, details: serde_json::Value) {
        let count = if condition {
            self.pass_count.fetch_add(1, atomic::Ordering::AcqRel)
        } else {
            self.fail_count.fetch_add(1, atomic::Ordering::AcqRel)
        };
        // only emit on the first pass or fail
        if count == 0 {
            dispatch::emit(Event::EmitEntry { entry: self, condition, details });
        }
    }

    #[inline]
    pub fn expectation(&self) -> Expectation {
        self.expectation
    }

    #[inline]
    pub fn property(&self) -> &'static str {
        self.property
    }

    #[inline]
    pub fn location(&self) -> &'static Location<'static> {
        self.location
    }

    #[inline]
    pub fn module(&self) -> &'static str {
        self.module
    }

    #[inline]
    pub fn function(&self) -> &'static str {
        self.function
    }

    #[inline]
    pub fn pass_count(&self) -> usize {
        self.pass_count.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn fail_count(&self) -> usize {
        self.fail_count.load(atomic::Ordering::Acquire)
    }
}
