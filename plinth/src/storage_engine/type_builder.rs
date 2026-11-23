use arrow::array::{Array, ArrayBuilder};

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
///     type ArrayType = PrimitiveArray<Int64Type>;
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
///     type ArrayType = PrimitiveArray<Int64Type>;
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
    type ArrayType: Array + 'static;

    /// Returns a fresh, empty builder instance.
    fn builder() -> Self::Builder;
}

mod primitive_impls {
    use arrow::array::{
        BooleanArray, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, PrimitiveArray, UInt8Builder, UInt16Builder, UInt32Builder,
        UInt64Builder,
        types::{
            Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
            UInt16Type, UInt32Type, UInt64Type,
        },
    };

    use crate::storage_engine::chunk::CHUNK_SIZE;

    use super::{Append, AppendableType};

    macro_rules! impl_primitive_appendable {
        ($(($native:ty, $builder:ty, $array:ty)),* $(,)?) => {
            $(
                impl AppendableType for $native {
                    type Builder = $builder;
                    type ArrayType = $array;
                    fn builder() -> Self::Builder { <$builder>::with_capacity(CHUNK_SIZE.as_usize()) }
                }
                impl AppendableType for Option<$native> {
                    type Builder = $builder;
                    type ArrayType = $array;
                    fn builder() -> Self::Builder { <$builder>::with_capacity(CHUNK_SIZE.as_usize()) }
                }
                impl Append<$native> for $builder {
                    type Element = $native;
                    #[inline(always)]
                    fn append(&mut self, value: $native) { self.append_value(value); }
                    #[inline(always)]
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
        (bool, BooleanBuilder, BooleanArray),
        (i8, Int8Builder, PrimitiveArray<Int8Type>),
        (i16, Int16Builder, PrimitiveArray<Int16Type>),
        (i32, Int32Builder, PrimitiveArray<Int32Type>),
        (i64, Int64Builder, PrimitiveArray<Int64Type>),
        (u8, UInt8Builder, PrimitiveArray<UInt8Type>),
        (u16, UInt16Builder, PrimitiveArray<UInt16Type>),
        (u32, UInt32Builder, PrimitiveArray<UInt32Type>),
        (u64, UInt64Builder, PrimitiveArray<UInt64Type>),
        (f32, Float32Builder, PrimitiveArray<Float32Type>),
        (f64, Float64Builder, PrimitiveArray<Float64Type>),
    );
}
