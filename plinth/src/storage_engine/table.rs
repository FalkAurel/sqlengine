use std::{collections::HashMap, marker::PhantomData};

use crate::{
    storage_engine::{chunk::{Append, AppendableType}, column::{Column, InvalidDowncast}, table::{
        schema::SchemaList,
        sealed::{Index, IndexNotFound},
    }, units::{LogicalOffset, VersionID}}, units::LogicalSize,
};

mod sealed {
    use crate::{
        storage_engine::table::{Table, TableRow},
        storage_engine::units::LogicalOffset,
    };

    #[derive(Debug)]
    pub struct IndexNotFound;

    pub trait Index {
        #[allow(private_interfaces)]
        fn resolve<T: TableRow>(&self, table: &Table<T>) -> Result<LogicalOffset, IndexNotFound>;
    }

    impl Index for &'static str {
        #[allow(private_interfaces)]
        fn resolve<T: TableRow>(&self, table: &Table<T>) -> Result<LogicalOffset, IndexNotFound> {
            table
                .column_resolver
                .get(self)
                .copied()
                .ok_or(IndexNotFound)
        }
    }

    impl Index for usize {
        #[allow(private_interfaces)]
        fn resolve<T: TableRow>(&self, table: &Table<T>) -> Result<LogicalOffset, IndexNotFound> {
            if *self < table.columns.len() {
                Ok(LogicalOffset::new(*self as u64))
            } else {
                Err(IndexNotFound)
            }
        }
    }
}

#[derive(Debug)]
pub enum VisitorError {
    IndexNotFound(IndexNotFound),
    InvalidDowncast(InvalidDowncast),
    /// Two column iterators reported different lengths.
    ///
    /// All columns in a streaming insert must yield the same number of values.
    /// The first column's iterator length is used as the expected length for
    /// all subsequent columns.
    LengthMismatch { expected: LogicalSize, got: LogicalSize },
}

pub trait FieldVisitor {
    fn visit_field<I: Index, V: AppendableType>(
        &mut self,
        index: I,
        value: <<V as AppendableType>::Builder as Append<V>>::Element,
    ) -> Result<(), VisitorError>;
}

/// Use when values arrive sequentially from an external source — streaming
/// from a file, walking a tree, or any case where a contiguous slice is not
/// available.
///
/// Unlike [`FieldVisitor`], which downcasts the column's type-erased builder
/// on every call, `StreamingFieldVisitor` resolves the index and downcasts
/// once, then hands the entire iterator to the column in a single write.
/// This amortizes both the index lookup and the downcast across all values,
/// making it strictly cheaper than calling `visit_field` in a loop.
///
/// When data is already in contiguous memory, prefer [`SliceFieldVisitor`]
/// instead — it maps directly to Arrow's `append_slice` and is only memory
/// bandwidth limited. For single-value writes, use [`FieldVisitor`].
pub trait StreamingFieldVisitor {
    fn visit_fields<I: Index, V: AppendableType>(
        &mut self,
        index: I,
        values: impl ExactSizeIterator<Item = <<V as AppendableType>::Builder as Append<V>>::Element>,
    ) -> Result<(), VisitorError>;
}

/// The fastest path for bulk inserts when data is already in contiguous memory.
///
/// For primitive column types, `visit_slice` maps directly to Arrow's
/// `append_slice`, which is a `memcpy`-equivalent over the underlying buffer.
/// The index lookup and downcast are paid once per column, not per value, so
/// the only remaining cost is memory bandwidth — there is no per-element
/// overhead beyond copying the data itself.
///
/// This is the preferred path for transferring large amounts of data into the
/// table. Use [`StreamingFieldVisitor`] when a contiguous slice is not
/// available, or [`FieldVisitor`] for single-value writes.
pub trait SliceFieldVisitor {
    fn visit_slice<const N: usize, I: Index, V: AppendableType>(
        &mut self,
        index: I,
        values: &[<<V as AppendableType>::Builder as Append<V>>::Element; N],
    ) -> Result<(), VisitorError>;
}

pub(crate) struct SingleFieldVisitor<'a, T: TableRow> {
    table: &'a mut Table<T>,
}

impl<'a, T: TableRow> FieldVisitor for SingleFieldVisitor<'a, T> {
    fn visit_field<I: Index, V: AppendableType>(
        &mut self,
        index: I,
        value: <<V as AppendableType>::Builder as Append<V>>::Element,
    ) -> Result<(), VisitorError> {
        let index: LogicalOffset = index
            .resolve(self.table)
            .map_err(VisitorError::IndexNotFound)?;
        self.table
            .columns
            .get_mut(index.get() as usize)
            .expect("This shouldn't fail. Failing would mean that the index resolving is broken or our static table mapping is cooked")
            .write::<V>(std::iter::once(value))
            .map_err(VisitorError::InvalidDowncast)
    }
}

pub(crate) struct StreamingVisitor<'a, T: TableRow> {
    table: &'a mut Table<T>,
    expected_len: Option<LogicalSize>,
}

impl<'a, T: TableRow> StreamingFieldVisitor for StreamingVisitor<'a, T> {
    fn visit_fields<I: Index, V: AppendableType>(
        &mut self,
        index: I,
        values: impl ExactSizeIterator<Item = <<V as AppendableType>::Builder as Append<V>>::Element>,
    ) -> Result<(), VisitorError> {
        let len: LogicalSize = LogicalSize::new(values.len() as u64);

        match self.expected_len {
            None => self.expected_len = Some(len),
            Some(expected) if expected != len => {
                return Err(VisitorError::LengthMismatch { expected, got: len });
            }
            _ => {}
        }

        let index: LogicalOffset = index.resolve(self.table).map_err(VisitorError::IndexNotFound)?;

        self.table
            .columns
            .get_mut(index.get() as usize)
            .expect("This shouldn't fail. Failing would mean that the index resolving is broken or our static table mapping is cooked")
            .write::<V>(values)
            .map_err(VisitorError::InvalidDowncast)
    }
}

pub(crate) struct SliceVisitor<'a, T: TableRow> {
    table: &'a mut Table<T>,
}

impl<'a, T: TableRow> SliceFieldVisitor for SliceVisitor<'a, T> {
    fn visit_slice<const N: usize, I: Index, V: AppendableType>(
        &mut self,
        index: I,
        values: &[<<V as AppendableType>::Builder as Append<V>>::Element; N],
    ) -> Result<(), VisitorError> {
        let index: LogicalOffset = index.resolve(self.table).map_err(VisitorError::IndexNotFound)?;

        self.table
            .columns
            .get_mut(index.get() as usize)
            .expect("This shouldn't fail. Failing would mean that the index resolving is broken or our static table mapping is cooked")
            .write_values::<V>(values)
            .map_err(VisitorError::InvalidDowncast)
    }
}

/// Schema marker: declares which columns a row type maps to in a [`Table`].
///
/// `Schema` is a type-level linked list (`Node<Head, Tail>`) encoding the
/// column types in the order they were added to the builder.
/// `TableBuilder::finish` only compiles when the target type's `Schema`
/// matches the accumulated schema — wrong order or missing columns are type
/// errors, not runtime panics.
///
/// This trait is a pure marker. To enable insertion, also implement one or
/// more of [`RowInsert`], [`StreamingTableRow`], or [`SliceTableRow`].
pub trait TableRow {
    type Schema: SchemaList;
}

/// Single-row insertion path.
///
/// Implement this to drive each field of a row through a [`FieldVisitor`] one
/// value at a time. Each `visit_field` call requires two explicit type
/// parameters: the index kind (`&'static str` or `usize`) and the column type
/// `V`. A type mismatch is a compile error, not a runtime panic.
///
/// What to do with each `visit_field` result is up to the implementor — the
/// table imposes no policy. Discard with `let _ = ...`, propagate with `?`,
/// accumulate into a `Vec`, or panic in debug builds.
///
/// # Example
///
/// ```
/// use plinth::storage_engine::table::{Empty, FieldVisitor, Node, RowInsert, TableBuilder, TableRow};
///
/// struct UserRow { id: i32, age: u8 }
///
/// impl TableRow for UserRow {
///     // Schema grows like a stack — last column added sits at the head.
///     type Schema = Node<u8, Node<i32, Empty>>;
/// }
///
/// impl RowInsert for UserRow {
///     fn visit_fields<V: FieldVisitor>(self, visitor: &mut V) {
///         // Writing <&str, u8> for self.id (an i32) would not compile.
///         let _ = visitor.visit_field::<&str, i32>("id", self.id);
///         let _ = visitor.visit_field::<&str, u8>("age", self.age);
///     }
/// }
///
/// let mut table = TableBuilder::default()
///     .add::<i32>("id").unwrap()
///     .add::<u8>("age").unwrap()
///     .finish::<UserRow>();
///
/// table.insert(UserRow { id: 1, age: 30 });
/// table.insert(UserRow { id: 2, age: 25 });
/// ```
pub trait RowInsert: TableRow {
    fn visit_fields<V: FieldVisitor>(self, visitor: &mut V)
    where
        Self: Sized;
}

/// Columnar extension of [`TableRow`] for streaming inserts.
///
/// Instead of one row at a time, the implementor calls
/// [`StreamingFieldVisitor::visit_fields`] once per column, passing an
/// iterator of values for that column. The index lookup and downcast are paid
/// once per column, not per value.
///
/// Use this when values arrive sequentially but not as a contiguous slice.
/// When data is already in contiguous memory, prefer [`SliceTableRow`].
///
/// # Example
///
/// ```
/// use plinth::storage_engine::table::{
///     Empty, Node, StreamingFieldVisitor, StreamingTableRow, TableBuilder, TableRow,
///     VisitorError,
/// };
///
/// struct UserRow { id: i32, age: u8 }
///
/// impl TableRow for UserRow {
///     type Schema = Node<u8, Node<i32, Empty>>;
/// }
///
/// // A source that owns iterators for each column — e.g. values read from a
/// // file or decoded from a network stream where no contiguous buffer exists.
/// // ExactSizeIterator is required so the visitor can validate equal lengths
/// // across columns before writing.
/// struct UserStream {
///     ids: Box<dyn ExactSizeIterator<Item = i32>>,
///     ages: Box<dyn ExactSizeIterator<Item = u8>>,
/// }
///
/// impl TableRow for UserStream {
///     type Schema = Node<u8, Node<i32, Empty>>;
/// }
///
/// impl StreamingTableRow for UserStream {
///     fn visit_columns_streaming<V: StreamingFieldVisitor>(self, visitor: &mut V) -> Result<(), VisitorError> {
///         // ? exits early if lengths disagree — no partial writes.
///         visitor.visit_fields::<&str, i32>("id", self.ids)?;
///         visitor.visit_fields::<&str, u8>("age", self.ages)?;
///         Ok(())
///     }
/// }
///
/// let mut table = TableBuilder::default()
///     .add::<i32>("id").unwrap()
///     .add::<u8>("age").unwrap()
///     .finish::<UserRow>();
///
/// let stream = UserStream {
///     ids: Box::new([1i32, 2, 3].into_iter()),
///     ages: Box::new([20u8, 25, 30].into_iter()),
/// };
///
/// table.streaming_insert(stream).unwrap();
/// ```
pub trait StreamingTableRow: TableRow {
    fn visit_columns_streaming<V: StreamingFieldVisitor>(self, visitor: &mut V) -> Result<(), VisitorError>;
}

/// Columnar extension of [`TableRow`] for bulk inserts from contiguous memory.
///
/// The implementor calls [`SliceFieldVisitor::visit_slice`] once per column,
/// passing a `&[Element; N]`. For primitive types this maps directly to Arrow's
/// `append_slice` — a `memcpy` over the underlying buffer. The index lookup
/// and downcast are paid once per column; the only remaining cost is memory
/// bandwidth.
///
/// This is the preferred path for transferring large amounts of data into the
/// table. Use [`StreamingTableRow`] when a contiguous slice is not available.
///
/// # Example
///
/// ```
/// use plinth::storage_engine::table::{
///     Empty, Node, SliceFieldVisitor, SliceTableRow, TableBuilder, TableRow,
/// };
///
/// struct UserRow { id: i32, age: u8 }
///
/// impl TableRow for UserRow {
///     type Schema = Node<u8, Node<i32, Empty>>;
/// }
///
/// // N is the single source of truth for batch length. Both fields must be
/// // arrays of exactly N elements — a mismatch is a compile error.
/// // Construct via `try_new` to validate Vec lengths at the boundary;
/// // after that the type system guarantees equal-length columns.
/// struct UserBatch<const N: usize> {
///     ids: Box<[i32; N]>,
///     ages: Box<[u8; N]>,
/// }
///
/// impl<const N: usize> UserBatch<N> {
///     fn try_new(ids: Vec<i32>, ages: Vec<u8>) -> Option<Self> {
///         Some(Self {
///             ids: ids.into_boxed_slice().try_into().ok()?,
///             ages: ages.into_boxed_slice().try_into().ok()?,
///         })
///     }
/// }
///
/// impl<const N: usize> TableRow for UserBatch<N> {
///     type Schema = Node<u8, Node<i32, Empty>>;
/// }
///
/// impl<const N: usize> SliceTableRow for UserBatch<N> {
///     fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
///         // For primitive types, each call maps to append_slice — a memcpy
///         // over the Arrow buffer. One downcast per column, no per-value cost.
///         // Equal length is guaranteed by N — no runtime length check needed.
///         let _ = visitor.visit_slice::<N, &str, i32>("id", &*self.ids);
///         let _ = visitor.visit_slice::<N, &str, u8>("age", &*self.ages);
///     }
/// }
///
/// let mut table = TableBuilder::default()
///     .add::<i32>("id").unwrap()
///     .add::<u8>("age").unwrap()
///     .finish::<UserRow>();
///
/// let batch = UserBatch::<3>::try_new(
///     vec![1, 2, 3],
///     vec![20, 25, 30],
/// ).expect("column lengths must match");
///
/// table.bulk_insert(&batch);
/// ```
pub trait SliceTableRow: TableRow {
    fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V);
}

pub mod schema {
    use crate::storage_engine::chunk::AppendableType;

    pub trait SchemaList {}

    pub struct Empty;
    pub struct Node<Head: AppendableType, Tail: SchemaList>(std::marker::PhantomData<(Head, Tail)>);

    impl SchemaList for Empty {}
    impl<Head: AppendableType, Tail: SchemaList> SchemaList for Node<Head, Tail> {}
}

pub use schema::{Empty, Node};

#[derive(Debug)]
pub struct DuplicateField;

/// Builds a [`Table`] with a schema verified at compile time.
///
/// Each call to [`add`](TableBuilder::add) extends the builder's type parameter,
/// prepending the new column type onto a type-level linked list. [`finish`](TableBuilder::finish)
/// only compiles when the accumulated schema exactly matches `T::Schema` for the
/// target row type — a mismatch or wrong column order is a type error, not a
/// runtime panic.
pub struct TableBuilder<Schema> {
    columns: Vec<Column>,
    column_resolver: HashMap<&'static str, LogicalOffset>,
    _schema: PhantomData<Schema>,
}

impl Default for TableBuilder<Empty> {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            column_resolver: HashMap::new(),
            _schema: PhantomData,
        }
    }
}

impl<Schema: SchemaList> TableBuilder<Schema> {
    pub fn add<V: AppendableType>(
        mut self,
        id: &'static str,
    ) -> Result<TableBuilder<Node<V, Schema>>, DuplicateField> {
        if self.column_resolver.contains_key(id) {
            return Err(DuplicateField);
        }

        self.column_resolver
            .insert(id, LogicalOffset::new(self.columns.len() as u64));

        self.columns
            .push(Column::new(Box::new(|| VersionID::new(0)), V::builder()));

        Ok(TableBuilder {
            columns: self.columns,
            column_resolver: self.column_resolver,
            _schema: PhantomData,
        })
    }

    pub fn finish<T: TableRow<Schema = Schema>>(self) -> Table<T> {
        Table {
            columns: self.columns.into_boxed_slice(),
            column_resolver: self.column_resolver,
            _marker: PhantomData,
        }
    }
}

pub struct Table<T: TableRow> {
    columns: Box<[Column]>,
    column_resolver: HashMap<&'static str, LogicalOffset>,
    _marker: PhantomData<T>,
}

impl<T: RowInsert> Table<T> {
    pub fn insert(&mut self, value: T) {
        let mut visitor = SingleFieldVisitor { table: self };
        value.visit_fields(&mut visitor);
    }
}

impl<T: TableRow> Table<T> {
    pub fn streaming_insert<S: StreamingTableRow<Schema = T::Schema>>(
        &mut self,
        source: S,
    ) -> Result<(), VisitorError> {
        let mut visitor: StreamingVisitor<T> = StreamingVisitor { table: self, expected_len: None };
        source.visit_columns_streaming(&mut visitor)
    }

    pub fn bulk_insert<S: SliceTableRow<Schema = T::Schema>>(&mut self, source: &S) {
        let mut visitor: SliceVisitor<T> = SliceVisitor { table: self };
        source.visit_columns_slice(&mut visitor);
    }
}

#[cfg(test)]
mod test {
    use crate::storage_engine::{
        table::{
            Empty, FieldVisitor, Node, RowInsert, SliceFieldVisitor, SliceTableRow,
            StreamingFieldVisitor, StreamingTableRow, TableBuilder, TableRow, VisitorError,
        },
        units::LogicalSize,
    };

    struct UserRow;

    impl TableRow for UserRow {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    struct UserStream {
        ids: std::vec::IntoIter<i32>,
        ages: std::vec::IntoIter<u8>,
    }

    impl TableRow for UserStream {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl StreamingTableRow for UserStream {
        fn visit_columns_streaming<V: StreamingFieldVisitor>(
            self,
            visitor: &mut V,
        ) -> Result<(), VisitorError> {
            visitor.visit_fields::<&str, i32>("id", self.ids)?;
            visitor.visit_fields::<&str, u8>("age", self.ages)?;
            Ok(())
        }
    }

    fn make_table() -> super::Table<UserRow> {
        TableBuilder::default()
            .add::<i32>("id")
            .unwrap()
            .add::<u8>("age")
            .unwrap()
            .finish::<UserRow>()
    }

    #[test]
    fn streaming_insert_equal_lengths_succeeds() {
        let mut table = make_table();
        let stream = UserStream {
            ids: vec![1, 2, 3].into_iter(),
            ages: vec![20, 25, 30].into_iter(),
        };
        assert!(table.streaming_insert(stream).is_ok());
    }

    #[test]
    fn streaming_insert_second_column_shorter_returns_mismatch() {
        let mut table = make_table();
        let stream = UserStream {
            ids: vec![1, 2, 3].into_iter(),
            ages: vec![20, 25].into_iter(),
        };
        assert!(matches!(
            table.streaming_insert(stream),
            Err(VisitorError::LengthMismatch { expected, got })
                if expected == LogicalSize::new(3) && got == LogicalSize::new(2)
        ));
    }

    #[test]
    fn streaming_insert_second_column_longer_returns_mismatch() {
        let mut table = make_table();
        let stream = UserStream {
            ids: vec![1, 2].into_iter(),
            ages: vec![20, 25, 30].into_iter(),
        };
        assert!(matches!(
            table.streaming_insert(stream),
            Err(VisitorError::LengthMismatch { expected, got })
                if expected == LogicalSize::new(2) && got == LogicalSize::new(3)
        ));
    }

    #[test]
    fn streaming_insert_both_empty_succeeds() {
        let mut table = make_table();
        let stream = UserStream {
            ids: vec![].into_iter(),
            ages: vec![].into_iter(),
        };
        assert!(table.streaming_insert(stream).is_ok());
    }

    // --- wrong type writes ---

    struct WrongTypeStream {
        ids: std::vec::IntoIter<i64>, // column expects i32
    }

    impl TableRow for WrongTypeStream {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl StreamingTableRow for WrongTypeStream {
        fn visit_columns_streaming<V: StreamingFieldVisitor>(
            self,
            visitor: &mut V,
        ) -> Result<(), VisitorError> {
            visitor.visit_fields::<&str, i64>("id", self.ids)?;
            Ok(())
        }
    }

    #[test]
    fn streaming_insert_wrong_type_returns_invalid_downcast() {
        let mut table = make_table();
        let stream = WrongTypeStream {
            ids: vec![1i64, 2, 3].into_iter(),
        };
        assert!(matches!(
            table.streaming_insert(stream),
            Err(VisitorError::InvalidDowncast(_))
        ));
    }

    // --- invalid index ---

    struct BadStringIndexStream;

    impl TableRow for BadStringIndexStream {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl StreamingTableRow for BadStringIndexStream {
        fn visit_columns_streaming<V: StreamingFieldVisitor>(
            self,
            visitor: &mut V,
        ) -> Result<(), VisitorError> {
            visitor.visit_fields::<&str, i32>("nonexistent", vec![1].into_iter())?;
            Ok(())
        }
    }

    struct BadUsizeIndexStream;

    impl TableRow for BadUsizeIndexStream {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl StreamingTableRow for BadUsizeIndexStream {
        fn visit_columns_streaming<V: StreamingFieldVisitor>(
            self,
            visitor: &mut V,
        ) -> Result<(), VisitorError> {
            visitor.visit_fields::<usize, i32>(999, vec![1].into_iter())?;
            Ok(())
        }
    }

    #[test]
    fn streaming_insert_unknown_string_index_returns_index_not_found() {
        let mut table = make_table();
        assert!(matches!(
            table.streaming_insert(BadStringIndexStream),
            Err(VisitorError::IndexNotFound(_))
        ));
    }

    #[test]
    fn streaming_insert_out_of_bounds_usize_index_returns_index_not_found() {
        let mut table = make_table();
        assert!(matches!(
            table.streaming_insert(BadUsizeIndexStream),
            Err(VisitorError::IndexNotFound(_))
        ));
    }

    // --- insert (RowInsert) ---

    struct InsertInvalidStringIndex;

    impl TableRow for InsertInvalidStringIndex {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl RowInsert for InsertInvalidStringIndex {
        fn visit_fields<V: FieldVisitor>(self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_field::<&str, i32>("nonexistent", 42),
                Err(VisitorError::IndexNotFound(_))
            ));
        }
    }

    struct InsertInvalidUsizeIndex;

    impl TableRow for InsertInvalidUsizeIndex {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl RowInsert for InsertInvalidUsizeIndex {
        fn visit_fields<V: FieldVisitor>(self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_field::<usize, i32>(999, 42),
                Err(VisitorError::IndexNotFound(_))
            ));
        }
    }

    struct InsertWrongType;

    impl TableRow for InsertWrongType {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl RowInsert for InsertWrongType {
        fn visit_fields<V: FieldVisitor>(self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_field::<&str, i64>("id", 42i64),
                Err(VisitorError::InvalidDowncast(_))
            ));
        }
    }

    #[test]
    fn insert_unknown_string_index_returns_index_not_found() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<InsertInvalidStringIndex>();
        table.insert(InsertInvalidStringIndex);
    }

    #[test]
    fn insert_out_of_bounds_usize_index_returns_index_not_found() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<InsertInvalidUsizeIndex>();
        table.insert(InsertInvalidUsizeIndex);
    }

    #[test]
    fn insert_wrong_type_returns_invalid_downcast() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<InsertWrongType>();
        table.insert(InsertWrongType);
    }

    // --- bulk_insert (SliceTableRow) ---

    struct BulkInvalidStringIndex;

    impl TableRow for BulkInvalidStringIndex {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl SliceTableRow for BulkInvalidStringIndex {
        fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_slice::<1, &str, i32>("nonexistent", &[42]),
                Err(VisitorError::IndexNotFound(_))
            ));
        }
    }

    struct BulkInvalidUsizeIndex;

    impl TableRow for BulkInvalidUsizeIndex {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl SliceTableRow for BulkInvalidUsizeIndex {
        fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_slice::<1, usize, i32>(999, &[42]),
                Err(VisitorError::IndexNotFound(_))
            ));
        }
    }

    struct BulkWrongType;

    impl TableRow for BulkWrongType {
        type Schema = Node<u8, Node<i32, Empty>>;
    }

    impl SliceTableRow for BulkWrongType {
        fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
            assert!(matches!(
                visitor.visit_slice::<1, &str, i64>("id", &[42i64]),
                Err(VisitorError::InvalidDowncast(_))
            ));
        }
    }

    #[test]
    fn bulk_insert_unknown_string_index_returns_index_not_found() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<BulkInvalidStringIndex>();
        table.bulk_insert(&BulkInvalidStringIndex);
    }

    #[test]
    fn bulk_insert_out_of_bounds_usize_index_returns_index_not_found() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<BulkInvalidUsizeIndex>();
        table.bulk_insert(&BulkInvalidUsizeIndex);
    }

    #[test]
    fn bulk_insert_wrong_type_returns_invalid_downcast() {
        let mut table = TableBuilder::default()
            .add::<i32>("id").unwrap()
            .add::<u8>("age").unwrap()
            .finish::<BulkWrongType>();
        table.bulk_insert(&BulkWrongType);
    }
}
