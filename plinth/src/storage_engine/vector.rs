use std::ops::Range;

use arrow::array::Array;

use crate::storage_engine::units::{LogicalOffset, LogicalSize};

pub(crate) const VECTOR_SIZE: LogicalSize = LogicalSize::new(1024);

/// A typed, zero-copy view over a small logical range of an Arrow array.
///
/// There is deliberately no `dyn Array` here.
/// The concrete array type has already been resolved by `ChunkView`.
pub(crate) struct Vector<'a, A: Array> {
    data: &'a A,
    offset: LogicalOffset,
    size: LogicalSize,
}

impl<'a, A: Array> Vector<'a, A> {
    #[inline]
    pub(crate) const fn new(data: &'a A, offset: LogicalOffset, size: LogicalSize) -> Self {
        Self { data, offset, size }
    }

    #[inline]
    pub const fn array(&self) -> &'a A {
        self.data
    }

    #[inline]
    pub const fn offset(&self) -> LogicalOffset {
        self.offset
    }

    #[inline]
    pub const fn size(&self) -> LogicalSize {
        self.size
    }

    #[inline]
    pub const fn range(&self) -> Range<usize> {
        let start: usize = self.offset.get() as usize;
        let end: usize = start + self.size.get() as usize;

        start..end
    }
}

/// Produces ~1 KiB logical vectors from a typed 64K chunk.
pub(crate) struct VectorIter<'a, A: Array> {
    data: &'a A,
    current: usize,
    size: LogicalSize,
}

impl<'a, A: Array> VectorIter<'a, A> {
    #[inline]
    pub(crate) fn new(data: &'a A, size: LogicalSize) -> Self {
        Self {
            data,
            current: 0,
            size: size,
        }
    }
}

impl<'a, A: Array> Iterator for VectorIter<'a, A> {
    type Item = Vector<'a, A>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.size.get() as usize {
            return None;
        }

        let offset: usize = self.current;

        let end: usize = (offset + VECTOR_SIZE.get() as usize).min(self.size.get() as usize);

        self.current = end;

        Some(Vector::new(
            self.data,
            LogicalOffset::new(offset as u64),
            LogicalSize::new((end - offset) as u64),
        ))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining: usize = self.size.get() as usize - self.current;

        let vectors: usize =
            (remaining + VECTOR_SIZE.get() as usize - 1) / VECTOR_SIZE.get() as usize;

        (vectors, Some(vectors))
    }
}

impl<A: Array> ExactSizeIterator for VectorIter<'_, A> {}
