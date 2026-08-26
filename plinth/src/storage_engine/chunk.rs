use std::sync::{Arc, OnceLock, RwLock};

use arrow::array::{Array, ArrayBuilder, ArrowPrimitiveType, BooleanBufferBuilder, PrimitiveBuilder};

use crate::storage_engine::{
    units::{LogicalSize, VersionID},
    vector::VectorIter,
};

const CHUNK_SIZE: LogicalSize = LogicalSize::new(1024 * 64);

pub(crate) struct FrozenChunk {
    array: Box<dyn Array>,
    chunk_id: VersionID,
    logical_size: LogicalSize,
    next: OnceLock<Arc<FrozenChunk>>,
}

impl FrozenChunk {
    pub(crate) fn new(
        array: Box<dyn Array>,
        chunk_id: VersionID,
        logical_size: LogicalSize,
    ) -> Self {
        Self {
            array,
            chunk_id,
            logical_size,
            next: OnceLock::new(),
        }
    }

    /// Creates a typed view over the type-erased array.
    ///
    /// The downcast is performed once, before vectorization, rather than
    /// repeatedly for each vector.
    pub(crate) fn view<A: Array + 'static>(&self) -> Option<ChunkView<'_, A>> {
        let array: &A = self.array.as_any().downcast_ref::<A>()?;

        Some(ChunkView {
            array,
            logical_size: self.logical_size,
        })
    }

    pub(crate) fn chunk_id(&self) -> VersionID {
        self.chunk_id
    }

    pub(crate) fn logical_size(&self) -> LogicalSize {
        self.logical_size
    }

    pub(crate) fn next(&self) -> Option<&Arc<FrozenChunk>> {
        self.next.get()
    }
}

/// A typed view over a frozen chunk.
///
/// The underlying `dyn Array` has already been downcast to `A` before
/// this view is created.
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

pub(crate) struct MutableChunk {
    builder: Box<dyn ArrayBuilder>,
    chunk_id: VersionID,
}

impl MutableChunk {
    pub(crate) fn new(builder: Box<dyn ArrayBuilder>, chunk_id: VersionID) -> Self {
        Self { builder, chunk_id }
    }

    fn builder<B: ArrayBuilder + Append>(self) -> Result<ChunkWriter<B>, Self> {
        if self.builder.as_any().is::<B>() {
            // Check the type before consuming the type-erased builder so that
            // we can safely recover the concrete builder below.
            let builder: Box<B> = self
                .builder
                .into_box_any()
                .downcast::<B>()
                .expect("See above. Invariant is upheld");

            Ok(ChunkWriter {
                builder,
                chunk_id: self.chunk_id,
            })
        } else {
            Err(self)
        }
    }
}

pub(crate) struct ChunkWriter<B: ArrayBuilder + Append> {
    builder: Box<B>,
    chunk_id: VersionID,
}

impl<B: ArrayBuilder + Append> ChunkWriter<B> {
    /// Appends a value to the chunk if it has not reached `CHUNK_SIZE`.
    ///
    /// Consuming `self` makes the chunk writer a state transition:
    /// once the chunk is full, `append` returns `None` and the writer
    /// can no longer be used to append additional values.
    ///
    /// This guarantees a uniform chunk size and prevents appending
    /// beyond the configured capacity.
    pub fn append(mut self, value: <B as Append>::Element) -> Option<Self> {
        if self.builder.len() < CHUNK_SIZE.get() as usize {
            self.builder.append(value);
            Some(self)
        } else {
            None
        }
    }
}

pub trait Append {
    type Element;
    fn append(&mut self, value: Self::Element);
}

impl<T: ArrowPrimitiveType> Append for PrimitiveBuilder<T> {
    type Element = <T as ArrowPrimitiveType>::Native;
    fn append(&mut self, value: Self::Element) {
        self.append_value(value);
    }
}

impl Append for BooleanBufferBuilder {
    type Element = bool;
    fn append(&mut self, value: Self::Element) {
        self.append(value);
    }
}



#[cfg(test)]
mod test {
    use arrow::array::{ArrayBuilder, Float32Builder};

    use crate::storage_engine::{
        chunk::{CHUNK_SIZE, MutableChunk},
        units::VersionID,
    };

    #[test]
    fn append_values() {
        let float_builder: Box<dyn ArrayBuilder> = Box::new(Float32Builder::new());
        let mutable_chunk: MutableChunk = MutableChunk::new(float_builder, VersionID::new(0));

        if let Ok(mut chunk_writer) = mutable_chunk.builder::<Float32Builder>() {
            for value in 0..CHUNK_SIZE.get() {
                chunk_writer = chunk_writer.append(value as f32).unwrap();
            }

            assert!(chunk_writer.append(1.0).is_none());
        } else {
            assert!(false);
        }
    }
}
