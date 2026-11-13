use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{Array, ArrayBuilder},
    compute::concat,
};

use crate::storage_engine::{chunk::CHUNK_SIZE, units::LogicalSize, vector::VectorIter};

/// Per-transaction staging buffer for a `MutableChunk`.
///
/// Each `append_from_builder` call commits one transaction's rows as a frozen
/// `Arc<dyn Array>` snapshot into `arrays`. On chunk fill, `fuse_arrays` runs
/// one `concat` to produce the single contiguous `Arc<dyn Array>` for the
/// `FrozenChunk`. This defers the O(n) copy to publish time so the read path
/// never calls `finish_cloned`.
pub(crate) struct ChunkBuffer {
    arrays: Vec<Arc<dyn Array>>,
    size: LogicalSize,
    _marker: PhantomData<*const ()>,
}

// `ChunkBuffer` is `!Sync` by construction (PhantomData<*const ()>). Only one
// owner can hold it at a time; ownership is transferred between `MutableChunk`
// and `ChunkWriter`. It is safe to send across threads.
unsafe impl Send for ChunkBuffer {}

impl std::fmt::Debug for ChunkBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkBuffer")
            .field("arrays", &self.arrays.len())
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ExceedsChunkCapacity;

/// A builder that has been capacity-checked against a specific `ChunkBuffer`.
pub(crate) struct CheckedBuilder<B: ArrayBuilder + ?Sized> {
    pub(crate) builder: Box<B>,
}

impl ChunkBuffer {
    pub(crate) fn new() -> Self {
        Self {
            arrays: Vec::new(),
            size: LogicalSize::new(0),
            _marker: PhantomData,
        }
    }

    pub(crate) const fn size(&self) -> LogicalSize {
        self.size
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.size.as_usize() == 0
    }

    /// Verify that `builder` fits in the remaining capacity and wrap it.
    pub(crate) fn make_builder<B: ArrayBuilder + ?Sized>(
        &self,
        builder: Box<B>,
    ) -> Result<CheckedBuilder<B>, ExceedsChunkCapacity> {
        if self.size + LogicalSize::new(builder.len() as u64) > LogicalSize::new(CHUNK_SIZE.get()) {
            Err(ExceedsChunkCapacity)
        } else {
            Ok(CheckedBuilder { builder })
        }
    }

    /// Commit `builder`'s current rows as a frozen snapshot.
    ///
    /// Returns `Ok((Self, Box<B>))` when the chunk is not yet full (the builder
    /// is reset and returned for reuse). Returns `Err((Box<B>, Arc<dyn Array>))`
    /// when the chunk is exactly full — the fused frozen array is ready to
    /// publish.
    pub(crate) fn append_from_builder<B: ArrayBuilder + ?Sized>(
        mut self,
        mut builder: CheckedBuilder<B>,
    ) -> Result<(Self, Box<B>), (Box<B>, Arc<dyn Array>)> {
        self.size = self.size + LogicalSize::new(builder.builder.len() as u64);

        let array: Arc<dyn Array> = builder.builder.finish();
        self.arrays.push(array);

        if self.size == LogicalSize::new(CHUNK_SIZE.get()) {
            let fused: Arc<dyn Array> = self.fuse_arrays();
            Err((builder.builder, fused))
        } else {
            Ok((self, builder.builder))
        }
    }

    /// Clone the accumulated per-transaction `Arc`s into an owned snapshot.
    ///
    /// This is a sequence of `Arc::clone` calls — no data is copied.
    pub(crate) fn snapshot(&self) -> WriteBufferSnapshot {
        WriteBufferSnapshot(self.arrays.iter().cloned().collect())
    }

    /// Fuse all accumulated arrays into one contiguous array.
    ///
    /// Called exactly once per chunk, at publish time. All arrays in `self.arrays`
    /// came from the same concrete builder type, so `concat` is always homogeneous.
    fn fuse_arrays(&mut self) -> Arc<dyn Array> {
        if self.arrays.len() == 1 {
            return self.arrays[0].clone();
        }
        let refs: Vec<&dyn Array> = self.arrays.iter().map(|a| a.as_ref()).collect();
        concat(&refs).expect("fuse_arrays called with homogeneous arrays from the same builder type")
    }
}

/// An owned, point-in-time snapshot of a `MutableChunk`'s committed rows.
///
/// Holds one `Arc<dyn Array>` per committed transaction. Each `Arc` is a
/// reference count bump — no underlying data is copied. The snapshot remains
/// valid even after the originating `ChunkBuffer` is modified or dropped.
pub(crate) struct WriteBufferSnapshot(Box<[Arc<dyn Array>]>);

impl WriteBufferSnapshot {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Total number of rows across all committed arrays.
    pub(crate) fn len(&self) -> usize {
        self.0.iter().map(|a| a.len()).sum()
    }

    /// Iterate over the raw per-commit arrays.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<dyn Array>> {
        self.0.iter()
    }

    /// Yield one `VectorIter<A>` per committed array, zero-copy.
    ///
    /// Arrays that don't downcast to `A` are silently skipped (this should not
    /// happen in practice because all arrays in a chunk come from the same builder).
    pub(crate) fn vectors<A: Array + 'static>(&self) -> impl Iterator<Item = VectorIter<'_, A>> {
        self.0.iter().filter_map(|arc| {
            arc.as_any()
                .downcast_ref::<A>()
                .map(VectorIter::new)
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{PrimitiveBuilder},
        datatypes::UInt64Type,
    };

    use crate::storage_engine::{chunk::CHUNK_SIZE, writer_buffer::ChunkBuffer};

    #[test]
    fn empty_buffer() {
        let buffer = ChunkBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.size().as_usize(), 0);
    }

    #[test]
    fn filling_buffer_exactly_returns_fused_array() {
        let buffer = ChunkBuffer::new();
        let mut builder = PrimitiveBuilder::<UInt64Type>::new();
        for i in 0..CHUNK_SIZE.get() {
            builder.append_value(i);
        }
        let checked = match buffer.make_builder(Box::new(builder)) {
            Ok(c) => c,
            Err(_) => panic!("CHUNK_SIZE builder must fit in an empty buffer"),
        };
        assert!(buffer.append_from_builder(checked).is_err());
    }

    #[test]
    fn incremental_fill_accumulates_then_fuses() {
        let mut buffer = ChunkBuffer::new();
        let mut builder = PrimitiveBuilder::<UInt64Type>::new();
        builder.append_value(0);
        let checked = match buffer.make_builder(Box::new(builder)) {
            Ok(c) => c,
            Err(_) => panic!("single-element builder must fit"),
        };
        let (b, mut reused) = match buffer.append_from_builder(checked) {
            Ok(pair) => pair,
            Err(_) => panic!("one element must not fill the chunk"),
        };
        buffer = b;

        assert_eq!(buffer.size().as_usize(), 1);
        assert!(!buffer.is_empty());

        // Fill the rest — `reused` is already a Box<B>, no extra wrapping needed.
        for i in 1..CHUNK_SIZE.get() {
            reused.append_value(i);
        }
        let checked = match buffer.make_builder(reused) {
            Ok(c) => c,
            Err(_) => panic!("remaining capacity must accept the rest of the builder"),
        };
        let result = buffer.append_from_builder(checked);
        assert!(result.is_err(), "chunk should be full");
    }

    #[test]
    fn make_builder_rejects_oversized_builder() {
        let buffer = ChunkBuffer::new();
        let mut builder = PrimitiveBuilder::<UInt64Type>::new();
        for i in 0..=CHUNK_SIZE.get() {
            builder.append_value(i);
        }
        assert!(buffer.make_builder(Box::new(builder)).is_err());
    }

    #[test]
    fn snapshot_is_independent_of_buffer() {
        let mut buffer = ChunkBuffer::new();
        let mut builder = PrimitiveBuilder::<UInt64Type>::new();
        builder.append_value(42);
        let checked = match buffer.make_builder(Box::new(builder)) {
            Ok(c) => c,
            Err(_) => panic!("single-element builder must fit"),
        };
        let (b, _) = match buffer.append_from_builder(checked) {
            Ok(pair) => pair,
            Err(_) => panic!("one element must not fill the chunk"),
        };
        buffer = b;

        let snapshot = buffer.snapshot();
        assert_eq!(snapshot.0.len(), 1);

        // Write another batch to the buffer
        let mut builder2 = PrimitiveBuilder::<UInt64Type>::new();
        builder2.append_value(99);
        let checked2 = match buffer.make_builder(Box::new(builder2)) {
            Ok(c) => c,
            Err(_) => panic!("second element must fit"),
        };
        let _ = buffer.append_from_builder(checked2);

        // Snapshot taken before the second write still has only one array
        assert_eq!(snapshot.0.len(), 1);
    }
}
