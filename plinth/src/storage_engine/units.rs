use std::ops::Add;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LogicalSize(u64);

impl LogicalSize {
    pub const fn new(size: u64) -> Self {
        Self(size)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LogicalOffset(u64);

impl LogicalOffset {
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl Add<LogicalSize> for LogicalOffset {
    type Output = Self;
    fn add(self, rhs: LogicalSize) -> Self::Output {
        LogicalOffset::new(self.0 + rhs.0)
    }
}

impl Add<LogicalOffset> for LogicalOffset {
    type Output = Self;
    fn add(self, rhs: LogicalOffset) -> Self::Output {
        LogicalOffset::new(self.0 + rhs.0)
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VersionID(u64);

impl VersionID {
    pub(crate) const fn new(version: u64) -> Self {
        Self(version)
    }

    pub(crate) const fn get(&self) -> u64 {
        self.0
    }
}
