use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayBuilder, ArrowPrimitiveType, BooleanBufferBuilder, PrimitiveBuilder,
};

use crate::storage_engine::{
    units::{LogicalSize, VersionID},
    vector::VectorIter,
};

const CHUNK_SIZE: LogicalSize = LogicalSize::new(1024 * 64);

#[derive(Debug)]
pub(crate) struct FrozenChunk {
    array: Arc<dyn Array>,
    chunk_id: VersionID,
    next: OnceLock<Arc<FrozenChunk>>,
}

impl FrozenChunk {
    pub(crate) fn new(array: Arc<dyn Array>, chunk_id: VersionID) -> Self {
        Self {
            array,
            chunk_id,
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
            logical_size: CHUNK_SIZE,
        })
    }

    pub(crate) fn chunk_id(&self) -> VersionID {
        self.chunk_id
    }

    pub(crate) fn next(&self) -> Option<&Arc<FrozenChunk>> {
        self.next.get()
    }

    pub(crate) fn set_next(&self, value: Arc<FrozenChunk>) -> Result<(), Arc<FrozenChunk>> {
        self.next.set(value)
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
        VectorIter::new(self.array)
    }
}

pub(crate) struct MutableChunk {
    builder: Option<Box<dyn ArrayBuilder>>,
    chunk_id: VersionID,
}

impl MutableChunk {
    pub(crate) const fn new(builder: Box<dyn ArrayBuilder>, chunk_id: VersionID) -> Self {
        Self {
            builder: Some(builder),
            chunk_id,
        }
    }

    pub(crate) fn builder<B: ArrayBuilder + sealed::Append + Send>(
        mut self,
    ) -> Result<ChunkWriter<B>, Self> {
        let builder: &Box<dyn ArrayBuilder> = self
            .builder
            .as_ref()
            .expect("MutableChunk builder must be present before type resolution");

        if builder.as_any().is::<B>() {
            // Check the type before consuming the type-erased builder so that
            // we can safely recover the concrete builder below.
            let builder: Box<B> = self
                .builder
                .take()
                .expect("MutableChunk builder must be present after successful type check")
                .into_box_any()
                .downcast::<B>()
                .expect("builder type must match the type checked above");

            Ok(ChunkWriter {
                builder: Some(builder),
                chunk_id: self.chunk_id,
            })
        } else {
            Err(self)
        }
    }

    pub(crate) fn from<B: ArrayBuilder + sealed::Append + Send>(
        mut writer: ChunkWriter<B>,
    ) -> Self {
        Self {
            builder: Some(
                writer
                    .builder
                    .take()
                    .expect("ChunkWriter builder must be present when converting to MutableChunk"),
            ),
            chunk_id: writer.chunk_id,
        }
    }
}

pub(crate) struct ChunkWriter<B: ArrayBuilder + sealed::Append + Send> {
    builder: Option<Box<B>>,
    chunk_id: VersionID,
}

impl<B: ArrayBuilder + sealed::Append> ChunkWriter<B> {
    #[inline]
    fn builder_ref(&self) -> &B {
        self.builder
            .as_deref()
            .expect("ChunkWriter builder must be present")
    }

    #[inline]
    fn builder_mut(&mut self) -> &mut B {
        self.builder
            .as_deref_mut()
            .expect("ChunkWriter builder must be present")
    }

    /// Appends a value to the chunk if it has not reached `CHUNK_SIZE`.
    ///
    /// Consuming `self` makes the chunk writer a state transition:
    /// once the chunk is full, `append` returns the builder and the writer
    /// can no longer be used to append additional values.
    ///
    /// This guarantees a uniform chunk size and prevents appending
    /// beyond the configured capacity.
    pub fn append(mut self, value: B::Element) -> Result<Self, (Box<B>, VersionID)> {
        if self.builder_ref().len() < CHUNK_SIZE.get() as usize {
            self.builder_mut().append(value);
            Ok(self)
        } else {
            let builder: Box<B> = self
                .builder
                .take()
                .expect("ChunkWriter builder must be present when chunk is full");

            Err((builder, self.chunk_id))
        }
    }
}

pub(crate) use sealed::Append;
mod sealed {
    pub(crate) trait Append {
        type Element;
        fn append(&mut self, value: Self::Element);
    }
}

impl<T: ArrowPrimitiveType> sealed::Append for PrimitiveBuilder<T> {
    type Element = <T as ArrowPrimitiveType>::Native;
    fn append(&mut self, value: Self::Element) {
        self.append_value(value);
    }
}

impl sealed::Append for BooleanBufferBuilder {
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

            assert!(chunk_writer.append(1.0).is_err());
        } else {
            assert!(false);
        }
    }
}
