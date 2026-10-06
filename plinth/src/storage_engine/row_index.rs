use std::{marker::PhantomData, ops::Range};

use crate::{table::schema::SchemaList, units::LogicalOffset};

pub struct RowIndex<'table, Schema: SchemaList> {
    pub(crate) offset: LogicalOffset,
    _marker: PhantomData<&'table Schema>,
}

pub(crate) struct RowIndexGenerator<'table, Schema: SchemaList> {
    range: Range<u64>,
    _marker: PhantomData<&'table Schema>,
}

impl<'table, Schema: SchemaList> RowIndexGenerator<'table, Schema> {
    pub(crate) const fn new(range: Range<u64>) -> Self {
        Self {
            range,
            _marker: PhantomData,
        }
    }
}

impl<'table, Schema: SchemaList> Iterator for RowIndexGenerator<'table, Schema> {
    type Item = RowIndex<'table, Schema>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|offset| RowIndex {
            offset: LogicalOffset::new(offset),
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        RowMetadata,
        storage_engine::table::{
            Empty, Node, SliceFieldVisitor, SliceTableRow, TableBuilder, TableRow, VisitorError,
        },
    };

    #[derive(Default)]
    struct Meta;
    impl RowMetadata for Meta {
        fn is_alive(&self) -> bool {
            true
        }
    }

    struct Row;
    impl TableRow for Row {
        type Schema = Node<i32, Empty>;
    }

    struct Batch<const N: usize>([i32; N]);
    impl<const N: usize> TableRow for Batch<N> {
        type Schema = Node<i32, Empty>;
    }
    impl<const N: usize> SliceTableRow<N> for Batch<N> {
        fn visit_columns_slice<'a, V: SliceFieldVisitor<'a, N>>(
            &'a self,
            visitor: &mut V,
        ) -> Result<(), VisitorError> {
            visitor.visit_slice::<&str, i32>("id", &self.0)
        }
    }

    fn make_table() -> super::super::table::Table<Row, Meta> {
        TableBuilder::default()
            .with_column::<i32>("id")
            .unwrap()
            .finish::<Row>()
    }

    #[test]
    fn bulk_insert_yields_correct_count() {
        let mut table = make_table();
        let indices: Vec<_> = table
            .bulk_insert(&Batch([1, 2, 3]), Meta::default)
            .unwrap()
            .collect();
        assert_eq!(indices.len(), 3);
    }

    #[test]
    fn second_bulk_insert_is_allowed_while_holding_first_indices() {
        let mut table = make_table();

        let first: Vec<_> = table
            .bulk_insert(&Batch([10, 20]), Meta::default)
            .unwrap()
            .collect();

        // Table is not borrowed here — this must compile and run.
        let second: Vec<_> = table
            .bulk_insert(&Batch([30, 40]), Meta::default)
            .unwrap()
            .collect();

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn multiple_batches_accumulate_correct_total() {
        let mut table = make_table();

        let a: Vec<_> = table
            .bulk_insert(&Batch([1, 2, 3]), Meta::default)
            .unwrap()
            .collect();
        let b: Vec<_> = table
            .bulk_insert(&Batch([4, 5, 6]), Meta::default)
            .unwrap()
            .collect();
        let c: Vec<_> = table
            .bulk_insert(&Batch([7]), Meta::default)
            .unwrap()
            .collect();

        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 3);
        assert_eq!(c.len(), 1);

        // All three vecs are simultaneously live — compiler accepts this only
        // because 'table is the table's lifetime, not a per-call borrow.
        let total = a.len() + b.len() + c.len();
        assert_eq!(total, 7);
    }
}
