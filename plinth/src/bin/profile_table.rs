use std::ops::Range;

use plinth::table::{
    Empty, Node, SliceFieldVisitor, SliceTableRow, StreamingTableRow, Table, TableBuilder,
    TableRow, VisitorError,
};

fn main() {
    #[cfg(feature = "streaming")]
    streaming_append();

    slice_append();
}

struct UserStream {
    values: Range<i32>,
}

impl TableRow for UserStream {
    type Schema = Node<i32, Empty>;
}

impl StreamingTableRow for UserStream {
    fn visit_columns_streaming<'a, V: plinth::table::StreamingFieldVisitor<'a>>(
        self,
        visitor: &mut V,
    ) -> Result<(), plinth::table::VisitorError>
    where
        Self: 'a,
    {
        visitor.visit_fields::<usize, i32>(0, self.values)
    }
}

struct SliceBatch<const N: usize> {
    ids: Box<[i32; N]>,
    values: Box<[i64; N]>,
}

impl<const N: usize> TableRow for SliceBatch<N> {
    type Schema = Node<i64, Node<i32, Empty>>;
}

impl<const N: usize> SliceTableRow for SliceBatch<N> {
    fn visit_columns_slice<'a, V: SliceFieldVisitor<'a>>(
        &'a self,
        visitor: &mut V,
    ) -> Result<(), VisitorError> {
        let _ = visitor.visit_slice::<N, usize, i32>(0, &self.ids);
        let _ = visitor.visit_slice::<N, usize, i64>(1, &self.values);

        Ok(())
    }
}

fn slice_append() {
    const N: usize = 1024 * 1024;
    let ids: Box<[i32; N]> = (0..N as i32)
        .collect::<Vec<_>>()
        .into_boxed_slice()
        .try_into()
        .unwrap();
    let values: Box<[i64; N]> = (0..N as i64)
        .collect::<Vec<_>>()
        .into_boxed_slice()
        .try_into()
        .unwrap();
    let batch: SliceBatch<1048576> = SliceBatch::<N> { ids, values };

    // let mut table: Table<SliceBatch<N>> = TableBuilder::default()
    //     .add("ids")
    //     .unwrap()
    //     .add("values")
    //     .unwrap()
    //     .finish();

    for _ in 0..10_000 {
        let mut table: Table<SliceBatch<N>> = TableBuilder::default()
        .add("ids")
        .unwrap()
        .add("values")
        .unwrap()
        .finish();

        std::hint::black_box(table.bulk_insert(&batch).unwrap());
    }
}

#[cfg(feature = "streaming")]
fn streaming_append() {
    let mut user_stream: Table<UserStream> = TableBuilder::default()
        .add::<i32>("values")
        .unwrap()
        .finish();

    for _ in 0..10000 {
        std::hint::black_box(
            user_stream
                .streaming_insert(UserStream {
                    values: 0..(1024 * 1024),
                })
                .unwrap(),
        );
    }
}
