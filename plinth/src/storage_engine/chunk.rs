use std::sync::{Arc, OnceLock};

use arrow::array::Array;

use crate::storage_engine::{units::LogicalSize, vector::VectorIter};

pub(crate) struct FrozenChunk {
    array: Box<dyn Array>,
    chunk_id: u64,
    logical_size: LogicalSize,
    next: OnceLock<Arc<FrozenChunk>>,
}

impl FrozenChunk {
    pub(crate) fn new(array: Box<dyn Array>, chunk_id: u64, logical_size: LogicalSize) -> Self {
        Self {
            array,
            chunk_id,
            logical_size,
            next: OnceLock::new(),
        }
    }

    /// Type-erased -> typed transition.
    ///
    /// This is intentionally done once before vectorization,
    /// rather than once per Vector.
    pub(crate) fn view<A: Array + 'static>(&self) -> Option<ChunkView<'_, A>> {
        let array: &A = self.array.as_any().downcast_ref::<A>()?;

        Some(ChunkView {
            array,
            logical_size: self.logical_size,
        })
    }

    pub(crate) fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    pub(crate) fn logical_size(&self) -> LogicalSize {
        self.logical_size
    }

    pub(crate) fn next(&self) -> Option<&Arc<FrozenChunk>> {
        self.next.get()
    }
}

/// A typed view of a frozen chunk.
///
/// The downcast from `dyn Array` to `A` has already happened
/// before this object is created.
pub(crate) struct ChunkView<'a, A: Array> {
    array: &'a A,
    logical_size: LogicalSize,
}

impl<'a, A: Array> ChunkView<'a, A> {
    #[inline]
    pub(crate) fn array(&self) -> &'a A {
        self.array
    }

    #[inline]
    pub(crate) fn logical_size(&self) -> LogicalSize {
        self.logical_size
    }

    #[inline]
    pub(crate) fn vectors(&self) -> VectorIter<'a, A> {
        VectorIter::new(self.array, self.logical_size)
    }
}

/// The mutable chunk should normally be generic over its concrete builder.
///
/// This avoids a downcast on every insertion.
pub(crate) struct MutableChunk<B> {
    builder: B,
    chunk_id: u64,
}

impl<B> MutableChunk<B> {
    pub(crate) fn new(builder: B, chunk_id: u64) -> Self {
        Self { builder, chunk_id }
    }

    #[inline]
    pub(crate) fn builder(&self) -> &B {
        &self.builder
    }

    #[inline]
    pub(crate) fn builder_mut(&mut self) -> &mut B {
        &mut self.builder
    }

    #[inline]
    pub(crate) fn chunk_id(&self) -> u64 {
        self.chunk_id
    }
}
