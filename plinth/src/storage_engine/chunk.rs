use std::{
    debug_assert,
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use arrow::array::{Array, ArrayBuilder};

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

    pub(crate) fn builder<B: AppendableType>(mut self) -> Result<ChunkWriter<B>, Self> {
        let builder: &dyn ArrayBuilder = self
            .builder
            .as_deref()
            .expect("MutableChunk builder must be present before type resolution");

        if builder.as_any().is::<B::Builder>() {
            // Check the type before consuming the type-erased builder so that
            // we can safely recover the concrete builder below.
            let builder: Box<B::Builder> = self
                .builder
                .take()
                .expect("MutableChunk builder must be present after successful type check")
                .into_box_any()
                .downcast::<B::Builder>()
                .expect("builder type must match the type checked above");

            Ok(ChunkWriter {
                builder,
                chunk_id: self.chunk_id,
            })
        } else {
            Err(self)
        }
    }

    pub(crate) fn from<B: AppendableType>(writer: ChunkWriter<B>) -> Self {
        Self {
            builder: Some(writer.builder),
            chunk_id: writer.chunk_id,
        }
    }
}

pub(crate) struct ChunkWriter<B: AppendableType> {
    builder: Box<B::Builder>,
    chunk_id: VersionID,
}

impl<B: AppendableType> ChunkWriter<B> {
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
    pub(crate) fn append(
        mut self,
        value: <<B as AppendableType>::Builder as Append<B>>::Element,
    ) -> Result<Self, (Box<B::Builder>, VersionID)> {
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
    #[inline(always)]
    pub(crate) fn append_values(
        mut self,
        values: &[<<B as AppendableType>::Builder as Append<B>>::Element],
    ) -> Result<
        Self,
        (
            Box<B::Builder>,
            VersionID,
            &[<<B as AppendableType>::Builder as Append<B>>::Element],
        ),
    > {
        debug_assert!(
            self.builder.len() < CHUNK_SIZE.get() as usize,
            "State Machine should never enter a state where we have a full buffer but keep writing to it."
        );

        let capacity: usize = CHUNK_SIZE.get() as usize - self.builder.len();
        let (written, returnable) = values.split_at(values.len().min(capacity));
        self.builder.append_values(written);

        if self.builder.len() == CHUNK_SIZE.get() as usize {
            Err((self.builder, self.chunk_id, returnable))
        } else {
            Ok(self)
        }
    }
}

/// Describes how a builder appends values of type `V`.
///
/// The trait is parameterised on `V` so that one concrete Arrow builder can
/// handle multiple logical types. The canonical example is a builder serving
/// both `T` (non-nullable) and `Option<T>` (nullable): both map to the same
/// Arrow builder but produce different element types and different append
/// behaviour.
///
/// # Relationship with [`AppendableType`]
///
/// [`AppendableType`] goes from type → builder. `Append<V>` goes from builder
/// → behaviour for `V`. The two form a closed loop:
///
/// ```text
/// V: AppendableType  =>  V::Builder: ArrayBuilder + Append<V>
/// ```
///
/// # Implementing for a custom type
///
/// The most common case is mapping a newtype wrapper onto an existing Arrow
/// builder so no new builder type is needed:
///
/// ```no_run
/// use arrow::array::Int64Builder;
/// use plinth::{Append, AppendableType};
///
/// struct Metres(i64);
///
/// impl AppendableType for Metres {
///     type Builder = Int64Builder;
///     fn builder() -> Int64Builder { Int64Builder::new() }
/// }
///
/// impl Append<Metres> for Int64Builder {
///     type Element = Metres;
///
///     fn append(&mut self, v: Metres) {
///         self.append_value(v.0);
///     }
///
///     fn append_values(&mut self, vs: &[Metres]) {
///         for v in vs {
///             self.append_value(v.0);
///         }
///     }
/// }
/// ```
///
/// Prefer bulk Arrow operations (`append_slice`, `extend`) over element-wise
/// loops inside `append_values` wherever the builder exposes them.
pub trait Append<V: AppendableType> {
    /// The value type consumed per insertion.
    ///
    /// For non-nullable types this is `V` itself. For nullable wrappers like
    /// `Option<T>` it is `Option<T::Element>`.
    type Element: Send;

    /// Appends a single value to the builder.
    fn append(&mut self, value: Self::Element);

    /// Appends a contiguous slice of values to the builder.
    fn append_values(&mut self, values: &[Self::Element]);
}

/// Associates a Rust type with the Arrow builder that stores it.
///
/// Implementing this trait makes a type usable as a column type in a
/// [`Table`]. The engine resolves the correct builder at compile time through
/// the associated `Builder` type, so a type mismatch between a column
/// declaration and a write call is a compile error, not a runtime panic.
///
/// # Relationship with [`Append`]
///
/// `AppendableType` names the builder; [`Append<Self>`] teaches that builder
/// how to accept values of this type. Both must be implemented together.
///
/// # Built-in implementations
///
/// All Arrow primitive scalars and their nullable counterparts are provided
/// out of the box:
///
/// | Rust type              | Arrow builder     |
/// |------------------------|-------------------|
/// | `bool` / `Option<bool>`| `BooleanBuilder`  |
/// | `i8` … `i64` (and `Option`) | `Int{8,16,32,64}Builder` |
/// | `u8` … `u64` (and `Option`) | `UInt{8,16,32,64}Builder` |
/// | `f32` / `f64` (and `Option`) | `Float{32,64}Builder` |
///
/// # Implementing for a custom type
///
/// ```no_run
/// use arrow::array::Int64Builder;
/// use plinth::{Append, AppendableType};
///
/// struct Metres(i64);
///
/// // Step 1 — teach Int64Builder to accept Metres
/// impl Append<Metres> for Int64Builder {
///     type Element = Metres;
///     fn append(&mut self, v: Metres) { self.append_value(v.0); }
///     fn append_values(&mut self, vs: &[Metres]) {
///         for v in vs { self.append_value(v.0); }
///     }
/// }
///
/// // Step 2 — register Metres as a column-capable type
/// impl AppendableType for Metres {
///     type Builder = Int64Builder;
///     fn builder() -> Int64Builder { Int64Builder::new() }
/// }
/// ```
///
/// If you also want nullable support, repeat both impls for `Option<Metres>`,
/// keeping `type Builder = Int64Builder`.
///
/// [`Table`]: crate::storage_engine::table::Table
pub trait AppendableType: Send + Sized {
    /// The Arrow builder used to accumulate values of this type.
    type Builder: ArrayBuilder + Append<Self>;

    /// Returns a fresh, empty builder instance.
    fn builder() -> Self::Builder;
}

mod primitive_impls {
    use arrow::array::{
        BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    };

    use crate::storage_engine::chunk::CHUNK_SIZE;

    use super::{Append, AppendableType};

    macro_rules! impl_primitive_appendable {
        ($(($native:ty, $builder:ty)),* $(,)?) => {
            $(
                impl AppendableType for $native {
                    type Builder = $builder;
                    fn builder() -> Self::Builder { <$builder>::with_capacity(CHUNK_SIZE.as_usize() * 20) }
                }
                impl AppendableType for Option<$native> {
                    type Builder = $builder;
                    fn builder() -> Self::Builder { <$builder>::with_capacity(CHUNK_SIZE.as_usize() * 20) }
                }
                impl Append<$native> for $builder {
                    type Element = $native;
                    fn append(&mut self, value: $native) { self.append_value(value); }
                    fn append_values(&mut self, values: &[$native]) { self.append_slice(values); }
                }
                impl Append<Option<$native>> for $builder {
                    type Element = Option<$native>;
                    fn append(&mut self, value: Option<$native>) { self.append_option(value); }
                    fn append_values(&mut self, values: &[Option<$native>]) {
                        for &value in values { self.append_option(value); }
                    }
                }
            )*
        };
    }

    impl_primitive_appendable!(
        (bool, BooleanBuilder),
        (i8, Int8Builder),
        (i16, Int16Builder),
        (i32, Int32Builder),
        (i64, Int64Builder),
        (u8, UInt8Builder),
        (u16, UInt16Builder),
        (u32, UInt32Builder),
        (u64, UInt64Builder),
        (f32, Float32Builder),
        (f64, Float64Builder),
    );
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayBuilder, BooleanBuilder, Float32Array, Float32Builder, Int64Array,
    };

    use crate::storage_engine::{
        chunk::{AppendableType, CHUNK_SIZE, ChunkWriter, FrozenChunk, MutableChunk},
        units::{LogicalOffset, VersionID},
    };

    fn unwrap_builder<V: AppendableType>(chunk: MutableChunk) -> ChunkWriter<V> {
        match chunk.builder::<V>() {
            Ok(writer) => writer,
            Err(_) => panic!("builder type resolution failed"),
        }
    }

    #[test]
    fn append_values() {
        let builder: Box<dyn ArrayBuilder> = Box::new(Float32Builder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut writer = unwrap_builder::<f32>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(Option::<i64>::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(42));

        let mut writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));

        let writer = unwrap_builder::<i64>(mutable_chunk);

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn builder_type_resolution_fails_for_wrong_type() {
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));

        let result = mutable_chunk.builder::<f32>();

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
        let writer = unwrap_builder::<i64>(mutable_chunk);

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn mutable_chunk_round_trip_preserves_builder() {
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(123));

        let mut writer = unwrap_builder::<i64>(mutable_chunk);

        writer = writer.append(10).unwrap();
        writer = writer.append(20).unwrap();
        writer = writer.append(30).unwrap();

        let mutable_chunk = MutableChunk::from(writer);
        let writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut chunk_writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));
        let writer = unwrap_builder::<i64>(mutable_chunk);

        let values: Vec<i64> = (0..10).collect();
        let writer = writer
            .append_values(&values)
            .expect("slice smaller than capacity must return Ok");

        assert_eq!(writer.builder.len(), 10);
    }

    #[test]
    fn append_values_exactly_fills_chunk() {
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(7));
        let writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(3));
        let writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(5));
        let mut writer = unwrap_builder::<i64>(mutable_chunk);

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
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));
        let writer = unwrap_builder::<i64>(mutable_chunk);

        let writer = writer
            .append_values(&[])
            .expect("empty slice must return Ok");

        assert_eq!(writer.builder.len(), 0);
    }

    #[test]
    fn boolean_builder_resolves_and_appends() {
        let builder: Box<dyn ArrayBuilder> = Box::new(BooleanBuilder::new());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(0));

        let mut writer = unwrap_builder::<bool>(mutable_chunk);

        writer = writer.append(true).unwrap();
        writer = writer.append(false).unwrap();
        writer = writer.append(true).unwrap();

        assert_eq!(writer.builder.len(), 3);
    }

    #[test]
    fn chunk_id_survives_mutable_to_frozen_transition() {
        let builder: Box<dyn ArrayBuilder> = Box::new(i64::builder());
        let mutable_chunk = MutableChunk::new(builder, VersionID::new(1234));

        let mut writer = unwrap_builder::<i64>(mutable_chunk);

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
