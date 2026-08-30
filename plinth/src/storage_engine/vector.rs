use std::ops::Range;

use arrow::array::{Array, ArrowPrimitiveType, PrimitiveArray};

use crate::storage_engine::{
    units::{LogicalOffset, LogicalSize},
    vector::sealed::Windowed,
};

pub(crate) const VECTOR_SIZE: LogicalSize = LogicalSize::new(1024);

/// A typed, zero-copy view over a small logical range of an Arrow array.
///
/// There is deliberately no `dyn Array` here.
/// The concrete array type has already been resolved by `ChunkView`.
pub struct Vector<'a, A: Array> {
    data: &'a A,
    range: Range<LogicalOffset>,
}

pub struct Validity<'a, A: Array> {
    data: &'a A,
    base: LogicalOffset,
}

impl<'a, A: Windowed> Validity<'a, A> {
    #[inline]
    pub fn is_valid(&self, offset: LogicalOffset) -> bool {
        Windowed::is_valid(self.data, self.base + offset)
    }
}

impl<'a, A: Array + sealed::Windowed> Vector<'a, A> {
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&<A as sealed::Windowed>::Window, Validity<'_, A>) -> R,
    {
        f(
            &self.data.window(&self.range),
            Validity {
                data: self.data,
                base: self.range.start,
            },
        )
    }
}

/// Produces 1024 element sized logical vectors from a typed chunk.
pub struct VectorIter<'a, A: Array> {
    data: &'a A,
    current: LogicalOffset,
}

impl<'a, A: Array> VectorIter<'a, A> {
    #[inline]
    pub(crate) fn new(data: &'a A) -> Self {
        Self {
            data,
            current: LogicalOffset::new(0),
        }
    }
}

impl<'a, A: Array + sealed::Windowed> Iterator for VectorIter<'a, A> {
    type Item = Vector<'a, A>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current.get() >= self.data.len() as u64 {
            return None;
        }

        let start: LogicalOffset = self.current;
        let end: LogicalOffset =
            LogicalOffset::new((start + VECTOR_SIZE).get().min(self.data.len() as u64));

        self.current = end;

        Some(Vector {
            data: self.data,
            range: start..end,
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining: u64 = self.data.len() as u64 - self.current.get();
        let vector_size: u64 = VECTOR_SIZE.get();

        let vectors: usize = remaining
            .div_ceil(vector_size)
            .try_into()
            .expect("vector count exceeds usize");

        (vectors, Some(vectors))
    }
}

mod sealed {
    use arrow::array::Array;

    use crate::storage_engine::units::LogicalOffset;
    use std::ops::Range;
    pub trait Windowed: Array {
        type Window: ?Sized;

        fn window(&self, range: &Range<LogicalOffset>) -> &Self::Window;
        fn is_valid(&self, index: LogicalOffset) -> bool {
            Array::is_valid(self, index.get() as usize)
        }
    }
}

impl<T: ArrowPrimitiveType> sealed::Windowed for PrimitiveArray<T> {
    type Window = [<T as ArrowPrimitiveType>::Native];

    #[inline]
    fn window(&self, range: &Range<LogicalOffset>) -> &Self::Window {
        let start: usize = range.start.get() as usize;
        let end: usize = range.end.get() as usize;

        &self.values()[start..end]
    }
}

impl<A: Array + sealed::Windowed> ExactSizeIterator for VectorIter<'_, A> {}
