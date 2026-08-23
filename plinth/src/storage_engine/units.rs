#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Copy)]
pub(crate) struct LogicalOffset(u64);

impl LogicalOffset {
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}
