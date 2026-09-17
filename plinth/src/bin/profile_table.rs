use std::{
    ops::Range,
    time::{Duration, Instant},
};

use arrow::array::{Int32Builder, Int64Builder};
use plinth::table::{
    Empty, Node, SliceFieldVisitor, SliceTableRow, StreamingTableRow, Table, TableBuilder,
    TableRow, VisitorError,
};

const N: usize = 1024 * 1024;
const CHUNK_SIZE: usize = 64 * 1024;

fn main() {
    slice_append();
    arrow_append();
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
    #[inline(always)]
    fn visit_columns_slice<'a, V: SliceFieldVisitor<'a>>(
        &'a self,
        visitor: &mut V,
    ) -> Result<(), VisitorError> {
        visitor.visit_slice::<N, usize, i32>(0, &self.ids)?;
        visitor.visit_slice::<N, usize, i64>(1, &self.values)?;

        Ok(())
    }
}

fn make_batch() -> SliceBatch<N> {
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

    SliceBatch { ids, values }
}

fn slice_append() {
    let batch = make_batch();

    let mut table_total = Duration::ZERO;

    for _ in 0..10_000 {
        let mut table: Table<SliceBatch<N>> = TableBuilder::default()
            .with_column::<i32>("ids")
            .unwrap()
            .with_column::<i64>("values")
            .unwrap()
            .finish();

        let start = Instant::now();

        std::hint::black_box(table.bulk_insert(&batch)).unwrap();

        table_total += start.elapsed();

        std::hint::black_box(table);
    }

    println!("Table bulk_insert: {:?} per 1M rows", table_total / 10_000);
}

fn arrow_append() {
    let mut arrow_total = Duration::ZERO;
    let batch = make_batch();

    for _ in 0..10_000 {
        let mut i32_builder = Int32Builder::with_capacity(CHUNK_SIZE);
        let mut i64_builder = Int64Builder::with_capacity(CHUNK_SIZE);

        let start = Instant::now();

        for start_idx in (0..N).step_by(CHUNK_SIZE) {
            let end = (start_idx + CHUNK_SIZE).min(N);

            i32_builder.append_slice(std::hint::black_box(&batch.ids[start_idx..end]));

            i64_builder.append_slice(std::hint::black_box(&batch.values[start_idx..end]));

            std::hint::black_box(i32_builder.finish());
            std::hint::black_box(i64_builder.finish());
        }

        arrow_total += start.elapsed();
    }

    println!("Raw Arrow append: {:?} per 1M rows", arrow_total / 10_000);
}

fn streaming_append() {
    let mut table: Table<UserStream> = TableBuilder::default()
        .with_column::<i32>("values")
        .unwrap()
        .finish();

    for _ in 0..10_000 {
        std::hint::black_box(table.streaming_insert(UserStream {
            values: 0..N as i32,
        }))
        .unwrap();
    }
}
