use std::{
    alloc::{Layout, alloc, dealloc},
    ptr::NonNull,
    slice,
};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct ByteOffset(usize);

impl ByteOffset {
    pub(crate) const fn new(offset: usize) -> Self {
        Self(offset)
    }

    pub(crate) const fn get(&self) -> usize {
        self.0
    }
}

#[derive(Debug)]
pub(crate) struct Buffer {
    data: NonNull<u8>,
    layout: Layout,
}

impl Buffer {
    pub(crate) fn new(layout: Layout) -> Option<Self> {
        tracing::info!("Allocating Buffer with {:?}", layout);
        let data = NonNull::new(unsafe { alloc(layout) })?;
        Some(Self { data, layout })
    }

    pub(crate) fn read(&self, offset: ByteOffset, length: usize) -> &[u8] {
        debug_assert!(offset.0 + length < self.layout.size());
        unsafe { slice::from_raw_parts(self.data.as_ptr().cast_const().add(offset.0), length) }
    }

    pub(crate) fn write(&self, index: ByteOffset, data: &[u8]) {
        assert!(data.len() + index.0 < self.layout.size());

        unsafe {
            self.data
                .as_ptr()
                .add(index.0)
                .copy_from(data.as_ptr(), data.len())
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        tracing::info!("Deallocating Buffer {:?}", self.layout);
        unsafe {
            dealloc(self.data.as_ptr(), self.layout);
        }
    }
}
