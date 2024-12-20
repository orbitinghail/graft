use core::fmt::Debug;
use std::fmt::Display;
use thiserror::ImplicitField;

#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CallerLocation(&'static core::panic::Location<'static>);

impl CallerLocation {
    #[inline]
    #[must_use]
    #[track_caller]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn file(&self) -> &'static str {
        self.0.file()
    }

    pub fn line(&self) -> u32 {
        self.0.line()
    }

    pub fn column(&self) -> u32 {
        self.0.column()
    }
}

impl Default for CallerLocation {
    #[inline]
    #[must_use]
    #[track_caller]
    fn default() -> Self {
        Self(core::panic::Location::caller())
    }
}

impl ImplicitField for CallerLocation {
    #[inline]
    #[must_use]
    #[track_caller]
    fn generate() -> Self {
        Self::default()
    }
}

impl Debug for CallerLocation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CallerLocation")
            .field("file", &self.0.file())
            .field("line", &self.0.line())
            .field("column", &self.0.column())
            .finish()
    }
}

impl Display for CallerLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.0.file(), self.0.line(), self.0.column())
    }
}
