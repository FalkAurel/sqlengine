use std::{
    debug_assert,
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use arrow::array::{Array, ArrayBuilder, ArrowPrimitiveType, BooleanBuilder, PrimitiveBuilder};

use crate::storage_engine::{
    units::{LogicalSize, VersionID},
    vector::VectorIter,
};

pub(crate) const CHUNK_SIZE: LogicalSize = LogicalSize::new(1024 * 64);

#[derive(Debug)]
pub(crate) struct FrozenChunk {
    array: Arc<dyn Array>,
    chunk_id: VersionID,
    next: OnceLock<Arc<FrozenChunk>>,
}

impl FrozenChunk {
    pub(crate) const fn new(array: Arc<dyn Array>, chunk_id: VersionID) -> Self {
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

    pub(crate) fn get_snapshot(&self) -> Arc<dyn Array> {
        self.builder
            .as_ref()
            .expect("Invalid State. Make sure to not have an active ChunkWriter.")
            .finish_cloned()
    }

    pub(crate) fn builder<B: ArrayBuilder + sealed::Append + Send>(
        mut self,
    ) -> Result<ChunkWriter<B>, Self> {
        let builder: &dyn ArrayBuilder = self
            .builder
            .as_deref()
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
                builder,
                chunk_id: self.chunk_id,
            })
        } else {
            Err(self)
        }
    }

    pub(crate) fn from<B: ArrayBuilder + sealed::Append + Send>(writer: ChunkWriter<B>) -> Self {
        Self {
            builder: Some(writer.builder),
            chunk_id: writer.chunk_id,
        }
    }
}

pub(crate) struct ChunkWriter<B: ArrayBuilder + sealed::Append + Send> {
    builder: Box<B>,
    chunk_id: VersionID,
}

impl<B: ArrayBuilder + sealed::Append> ChunkWriter<B> {
    /// Appends a value to the chunk, consuming the value before checking
    /// whether the chunk has reached `CHUNK_SIZE`.
    ///
    /// Consuming `self` makes the chunk writer a state transition. The value
    /// is always inserted when the writer has capacity. If this insertion
    /// fills the chunk, `append` returns the completed builder together with
    /// its chunk ID instead of returning the writer.
    ///
    /// This ordering is important: checking whether the chunk has room for
    /// the value *before* inserting it would require the value to be retained
    /// so it could be inserted later. That would either require `B::Element`
    /// to implement `Clone`/`Copy` or risk losing a non-cloneable value when
    /// the chunk becomes full.
    ///
    /// Therefore, callers can rely on the invariant that every value passed
    /// to `append` is inserted exactly once, and `Err` means that the
    /// insertion just completed the chunk.
    #[inline(always)]
    pub(crate) fn append(mut self, value: B::Element) -> Result<Self, (Box<B>, VersionID)> {
        debug_assert!(
            self.builder.len() < CHUNK_SIZE.get() as usize,
            "State Machine should never enter a state where we have a full buffer but keep writing to it."
        );
        self.builder.append(value);

        if self.builder.len() < CHUNK_SIZE.get() as usize {
            Ok(self)
        } else {
            Err((self.builder, self.chunk_id))
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn append_values(
        mut self,
        values: &[B::Element],
    ) -> Result<Self, (Box<B>, VersionID, &[B::Element])> {
        debug_assert!(
            self.builder.len() < CHUNK_SIZE.get() as usize,
            "State Machine should never enter a state where we have a full buffer but keep writing to it."
        );

        let capacity: usize = CHUNK_SIZE.get() as usize - self.builder.len();
        let (written, returnable) = values.split_at(values.len().min(capacity));
        Append::append_values(self.builder.as_mut(), written);

        if self.builder.len() == CHUNK_SIZE.get() as usize {
            Err((self.builder, self.chunk_id, returnable))
        } else {
            Ok(self)
        }
    }
}

pub(crate) use sealed::Append;
mod sealed {
    pub(crate) trait Append {
        type Element;
        fn append(&mut self, value: Self::Element);
        fn append_values(&mut self, values: &[Self::Element]);
    }
}

impl<T: ArrowPrimitiveType> sealed::Append for PrimitiveBuilder<T> {
    type Element = <T as ArrowPrimitiveType>::Native;

    fn append(&mut self, value: Self::Element) {
        self.append_value(value);
    }

    fn append_values(&mut self, values: &[Self::Element]) {
        self.append_slice(values);
    }
}

impl sealed::Append for BooleanBuilder {
    type Element = bool;

    fn append(&mut self, value: Self::Element) {
        BooleanBuilder::append_value(self, value);
    }

    fn append_values(&mut self, values: &[Self::Element]) {
        self.append_slice(values);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayBuilder, BooleanBuilder, Float32Array, Float32Builder, Int64Array, Int64Builder,
    };

    use crate::storage_engine::{
        chunk::{Append, CHUNK_SIZE, ChunkWriter, FrozenChunk, MutableChunk},
        units::{LogicalOffset, VersionID},
    };

    fn unwrap_builder<B: ArrayBuilder + Append + Send>(chunk: MutableChunk) -> ChunkWriter<B> {
        match chunk.builder::<B>() {
            Ok(writer) => writer,
            Err(_) => panic!("builder type resolution failed"),
        }
    }

    #[test]
    fn append_values() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Float32Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut writer = unwrap_builder::<Float32Builder>(mutable_chunk);

        for value in 0..CHUNK_SIZE.get() - 1 {
            writer = writer
                .append(value as f32)
                .expect("chunk should not be full yet");
        }

        let result = writer.append(CHUNK_SIZE.get() as f32 - 1.0);

        assert!(
            result.is_err(),
            "the append filling the chunk must return the completed builder"
        );

        let (builder, version_id) = match result {
            Err(values) => values,
            Ok(_) => panic!("State Machine in invalid state"),
        };

        assert_eq!(version_id, VersionID::new(0));
        assert_eq!(builder.len(), CHUNK_SIZE.get() as usize);
    }

    #[test]
    fn append_fills_exactly_one_chunk() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(42));

        let mut writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        for value in 0..CHUNK_SIZE.get() - 1 {
            writer = writer
                .append(value as i64)
                .expect("chunk should have capacity");
        }

        let (builder, version_id) = match writer.append((CHUNK_SIZE.get() - 1) as i64) {
            Ok(_) => panic!("the final value should complete the chunk"),
            Err(values) => values,
        };

        assert_eq!(version_id, VersionID::new(42));
        assert_eq!(builder.len(), CHUNK_SIZE.get() as usize);
    }

    #[test]
    fn builder_type_resolution_succeeds_for_matching_type() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));

        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn builder_type_resolution_fails_for_wrong_type() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));

        let result = mutable_chunk.builder::<Float32Builder>();

        assert!(
            result.is_err(),
            "resolving an Int64Builder as Float32Builder must fail"
        );

        let mutable_chunk = match result {
            Ok(_) => panic!("Type resolution should have failed"),
            Err(chunk) => chunk,
        };

        // The original builder must still be available after a failed
        // type resolution.
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn mutable_chunk_round_trip_preserves_builder() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(123));

        let mut writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        writer = writer.append(10).unwrap();
        writer = writer.append(20).unwrap();
        writer = writer.append(30).unwrap();

        let mutable_chunk = MutableChunk::from(writer);
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        assert_eq!(writer.builder.len(), 3);
    }

    #[test]
    fn frozen_chunk_exposes_chunk_id() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = FrozenChunk::new(array, VersionID::new(99));

        assert_eq!(chunk.chunk_id(), VersionID::new(99));
    }

    #[test]
    fn frozen_chunk_view_downcasts_correct_type() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = FrozenChunk::new(array, VersionID::new(0));

        let view = chunk
            .view::<Int64Array>()
            .expect("Int64Array downcast should succeed");

        assert_eq!(view.array().len(), 3);
    }

    #[test]
    fn frozen_chunk_view_rejects_wrong_type() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = FrozenChunk::new(array, VersionID::new(0));

        assert!(
            chunk.view::<Float32Array>().is_none(),
            "view should reject an incompatible array type"
        );
    }

    #[test]
    fn frozen_chunk_next_is_initially_empty() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = FrozenChunk::new(array, VersionID::new(0));

        assert!(chunk.next().is_none());
    }

    #[test]
    fn frozen_chunk_next_can_only_be_set_once() {
        let first = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![1])),
            VersionID::new(1),
        ));

        let second = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![2])),
            VersionID::new(2),
        ));

        let third = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![3])),
            VersionID::new(3),
        ));

        assert!(first.set_next(second.clone()).is_ok());

        let result = first.set_next(third);

        assert!(
            result.is_err(),
            "FrozenChunk::next must only be initialized once"
        );

        assert_eq!(
            first
                .next()
                .expect("next should have been initialized")
                .chunk_id(),
            VersionID::new(2)
        );
    }

    #[test]
    fn frozen_chunk_next_forms_chain() {
        let first = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![1])),
            VersionID::new(1),
        ));

        let second = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![2])),
            VersionID::new(2),
        ));

        let third = Arc::new(FrozenChunk::new(
            Arc::new(Int64Array::from(vec![3])),
            VersionID::new(3),
        ));

        first
            .set_next(second.clone())
            .expect("first next should be empty");

        second.set_next(third).expect("second next should be empty");

        assert_eq!(first.next().unwrap().chunk_id(), VersionID::new(2));

        assert_eq!(
            first.next().unwrap().next().unwrap().chunk_id(),
            VersionID::new(3)
        );

        assert!(first.next().unwrap().next().unwrap().next().is_none());
    }

    #[test]
    fn read_values() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut chunk_writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        for value in 0..CHUNK_SIZE.get() - 1 {
            chunk_writer = chunk_writer.append(value as i64).unwrap();
        }

        let (mut builder, version_id) = match chunk_writer.append(CHUNK_SIZE.get() as i64 - 1) {
            Ok(_) => panic!("final append should freeze the chunk"),
            Err(values) => values,
        };
        let array: Arc<dyn Array + 'static> = Arc::new(builder.finish());
        let frozen_chunk = FrozenChunk::new(array, version_id);

        let view = frozen_chunk
            .view::<Int64Array>()
            .expect("downcast should succeed");

        assert_eq!(view.array().len(), CHUNK_SIZE.get() as usize);

        let mut cum_sum = 0i64;

        for vector in view.vectors() {
            let sum: i64 = vector.with(|window, validity| {
                let mut sum = 0i64;

                for (index, element) in window.iter().enumerate() {
                    assert!(
                        validity.is_valid(LogicalOffset::new(index as u64)),
                        "all values in this test should be valid"
                    );

                    sum += *element;
                }

                sum
            });

            cum_sum += sum;
        }

        let n = CHUNK_SIZE.get() as i64 - 1;

        assert_eq!(cum_sum, n * (n + 1) / 2);
    }

    #[test]
    fn append_values_fits_within_chunk() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        let values: Vec<i64> = (0..10).collect();
        let writer = writer
            .append_values(&values)
            .expect("slice smaller than capacity must return Ok");

        assert_eq!(writer.builder.len(), 10);
    }

    #[test]
    fn append_values_exactly_fills_chunk() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64).collect();
        let (finished_builder, version_id, remainder) = match writer.append_values(&values) {
            Err(tuple) => tuple,
            Ok(_) => panic!("exact fill must return Err with empty remainder"),
        };

        assert_eq!(version_id, VersionID::new(7));
        assert_eq!(finished_builder.len(), CHUNK_SIZE.get() as usize);
        assert!(remainder.is_empty());
    }

    #[test]
    fn append_values_overflow_returns_remaining() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(3));
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        let overflow = 5usize;
        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64 + overflow as i64).collect();
        let (finished_builder, version_id, remaining) = match writer.append_values(&values) {
            Err(tuple) => tuple,
            Ok(_) => panic!("overflow must return Err with the remaining slice"),
        };

        assert_eq!(version_id, VersionID::new(3));
        assert_eq!(finished_builder.len(), CHUNK_SIZE.get() as usize);
        assert_eq!(remaining.len(), overflow);
        assert_eq!(remaining, &values[CHUNK_SIZE.get() as usize..]);
    }

    #[test]
    fn append_values_overflow_from_partial_chunk() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(5));
        let mut writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        let pre_filled = 10usize;
        for v in 0..pre_filled as i64 {
            writer = writer.append(v).expect("chunk should have capacity");
        }

        let overflow = 3usize;
        let remaining_capacity = CHUNK_SIZE.get() as usize - pre_filled;
        let values: Vec<i64> = (0..remaining_capacity as i64 + overflow as i64).collect();
        let (finished_builder, version_id, remaining) = match writer.append_values(&values) {
            Err(tuple) => tuple,
            Ok(_) => panic!("overflow must return Err"),
        };

        assert_eq!(version_id, VersionID::new(5));
        assert_eq!(finished_builder.len(), CHUNK_SIZE.get() as usize);
        assert_eq!(remaining.len(), overflow);
    }

    #[test]
    fn append_values_empty_slice_is_noop() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));
        let writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        let writer = writer
            .append_values(&[])
            .expect("empty slice must return Ok");

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn boolean_builder_resolves_and_appends() {
        let builder: Box<dyn ArrayBuilder> = Box::new(BooleanBuilder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut writer = unwrap_builder::<BooleanBuilder>(mutable_chunk);

        writer = writer.append(true).unwrap();
        writer = writer.append(false).unwrap();
        writer = writer.append(true).unwrap();

        assert_eq!(writer.builder.len(), 3);
    }

    #[test]
    fn chunk_id_survives_mutable_to_frozen_transition() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Int64Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(1234));

        let mut writer = unwrap_builder::<Int64Builder>(mutable_chunk);

        for value in 0..CHUNK_SIZE.get() - 1 {
            writer = writer.append(value as i64).unwrap();
        }

        let (mut builder, version_id) = match writer.append((CHUNK_SIZE.get() - 1) as i64) {
            Ok(_) => panic!("final append should complete the chunk"),
            Err(values) => values,
        };

        let array: Arc<dyn Array> = Arc::new(builder.finish());
        let frozen = FrozenChunk::new(array, version_id);

        assert_eq!(frozen.chunk_id(), VersionID::new(1234));
        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
    }
}
