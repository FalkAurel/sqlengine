use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use plinth::storage_engine::table::{
    Empty, Node, SliceFieldVisitor, SliceTableRow, StreamingFieldVisitor, StreamingTableRow, Table,
    TableBuilder, TableRow, VisitorError,
};
use std::hint::black_box;
use std::mem::size_of;
use std::time::Duration;

// -----------------------------------------------------------------------------
// Schemas
// -----------------------------------------------------------------------------

type BenchSchema = Node<i64, Node<i32, Empty>>;
type BenchSchemaSingle = Node<i32, Empty>;

struct BenchRow;

impl TableRow for BenchRow {
    type Schema = BenchSchema;
}

struct BenchRowSingle;

impl TableRow for BenchRowSingle {
    type Schema = BenchSchemaSingle;
}

// -----------------------------------------------------------------------------
// Tables
// -----------------------------------------------------------------------------

fn make_table() -> Table<BenchRow> {
    TableBuilder::default()
        .add::<i32>("id")
        .unwrap()
        .add::<i64>("value")
        .unwrap()
        .finish::<BenchRow>()
}

fn make_single_column_table() -> Table<BenchRowSingle> {
    TableBuilder::default()
        .add::<i32>("value")
        .unwrap()
        .finish::<BenchRowSingle>()
}

// -----------------------------------------------------------------------------
// Streaming: two columns
// -----------------------------------------------------------------------------

struct StreamBatch<I, J> {
    ids: I,
    values: J,
}

impl<I, J> TableRow for StreamBatch<I, J>
where
    I: ExactSizeIterator<Item = i32>,
    J: ExactSizeIterator<Item = i64>,
{
    type Schema = BenchSchema;
}

impl<I, J> StreamingTableRow for StreamBatch<I, J>
where
    I: ExactSizeIterator<Item = i32>,
    J: ExactSizeIterator<Item = i64>,
{
    fn visit_columns_streaming<V: StreamingFieldVisitor>(
        self,
        visitor: &mut V,
    ) -> Result<(), VisitorError> {
        visitor.visit_fields::<&str, i32>("id", self.ids)?;
        visitor.visit_fields::<&str, i64>("value", self.values)?;
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Bulk: two columns
// -----------------------------------------------------------------------------

struct SliceBatch<const N: usize> {
    ids: Box<[i32; N]>,
    values: Box<[i64; N]>,
}

impl<const N: usize> TableRow for SliceBatch<N> {
    type Schema = BenchSchema;
}

impl<const N: usize> SliceTableRow for SliceBatch<N> {
    fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
        let _ = visitor.visit_slice::<N, usize, i32>(0, &self.ids);
        let _ = visitor.visit_slice::<N, usize, i64>(1, &self.values);
    }
}

// -----------------------------------------------------------------------------
// Streaming: single column
// -----------------------------------------------------------------------------

struct StreamBatchSingle<I> {
    values: I,
}

impl<I> TableRow for StreamBatchSingle<I>
where
    I: ExactSizeIterator<Item = i32>,
{
    type Schema = BenchSchemaSingle;
}

impl<I> StreamingTableRow for StreamBatchSingle<I>
where
    I: ExactSizeIterator<Item = i32>,
{
    fn visit_columns_streaming<V: StreamingFieldVisitor>(
        self,
        visitor: &mut V,
    ) -> Result<(), VisitorError> {
        visitor.visit_fields::<usize, i32>(0, self.values)?;
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Bulk: single column
// -----------------------------------------------------------------------------

struct SliceBatchSingle<const N: usize> {
    values: Box<[i32; N]>,
}

impl<const N: usize> TableRow for SliceBatchSingle<N> {
    type Schema = BenchSchemaSingle;
}

impl<const N: usize> SliceTableRow for SliceBatchSingle<N> {
    fn visit_columns_slice<V: SliceFieldVisitor>(&self, visitor: &mut V) {
        let _ = visitor.visit_slice::<N, usize, i32>(0, &self.values);
    }
}

// -----------------------------------------------------------------------------
// Bench sizes
// -----------------------------------------------------------------------------

const SIZES: [usize; 5] = [1_024, 16_384, 65_536, 131_072, 1_048_576];

// -----------------------------------------------------------------------------
// Bench: streaming, two columns
// -----------------------------------------------------------------------------

pub fn bench_table_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_write");
    group.measurement_time(Duration::from_secs_f64(8.7));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(50);

    for &size in &SIZES {
        group.throughput(Throughput::Bytes(
            (size * (size_of::<i32>() + size_of::<i64>())) as u64,
        ));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut table = make_table();
                let batch = StreamBatch {
                    ids: (0..size as i32).map(|v| black_box(v)),
                    values: (0..size as i32).map(|v| black_box(v as i64)),
                };
                table.streaming_insert(batch).unwrap();
                black_box(table);
            });
        });
    }

    group.finish();
}

// -----------------------------------------------------------------------------
// Bench: bulk, two columns
// -----------------------------------------------------------------------------

macro_rules! bench_bulk_sizes {
    ($group:expr, [$($n:literal),+]) => {$(
        {
            const N: usize = $n;
            let ids: Box<[i32; N]> = (0..N as i32).collect::<Vec<_>>().into_boxed_slice().try_into().unwrap();
            let values: Box<[i64; N]> = (0..N as i64).collect::<Vec<_>>().into_boxed_slice().try_into().unwrap();
            let batch = SliceBatch::<N> { ids, values };
            $group.throughput(Throughput::Bytes(
                (N * (size_of::<i32>() + size_of::<i64>())) as u64,
            ));
            $group.bench_function(BenchmarkId::from_parameter(N), |b| {
                b.iter(|| {
                    let mut table = make_table();
                    table.bulk_insert(black_box(&batch));
                    black_box(table);
                });
            });
        }
    )+};
}

pub fn bench_table_write_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_write_values");
    group.measurement_time(Duration::from_secs_f64(8.7));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(50);
    bench_bulk_sizes!(group, [1_024, 16_384, 65_536, 131_072, 1_048_576]);
    group.finish();
}

// -----------------------------------------------------------------------------
// Bench: streaming, single column
// -----------------------------------------------------------------------------

pub fn bench_single_column_table_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_column_table_write");
    group.measurement_time(Duration::from_secs_f64(8.7));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(50);

    for &size in &SIZES {
        group.throughput(Throughput::Bytes((size * size_of::<i32>()) as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut table = make_single_column_table();
                let batch = StreamBatchSingle {
                    values: (0..size as i32).map(|v| black_box(v)),
                };
                table.streaming_insert(batch).unwrap();
                black_box(table);
            });
        });
    }

    group.finish();
}

// -----------------------------------------------------------------------------
// Bench: bulk, single column
// -----------------------------------------------------------------------------

macro_rules! bench_single_bulk_sizes {
    ($group:expr, [$($n:literal),+]) => {$(
        {
            const N: usize = $n;
            let values: Box<[i32; N]> = (0..N as i32).collect::<Vec<_>>().into_boxed_slice().try_into().unwrap();
            let batch = SliceBatchSingle::<N> { values };
            $group.throughput(Throughput::Bytes(
                (N * size_of::<i32>()) as u64,
            ));
            $group.bench_function(BenchmarkId::from_parameter(N), |b| {
                b.iter(|| {
                    let mut table = make_single_column_table();
                    table.bulk_insert(black_box(&batch));
                    black_box(table);
                });
            });
        }
    )+};
}

pub fn bench_single_column_table_write_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_column_table_write_values");
    group.measurement_time(Duration::from_secs_f64(8.7));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(50);
    bench_single_bulk_sizes!(group, [1_024, 16_384, 65_536, 131_072, 1_048_576]);
    group.finish();
}

// -----------------------------------------------------------------------------
// Criterion
// -----------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_table_write,
    bench_table_write_values,
    bench_single_column_table_write,
    bench_single_column_table_write_values,
);

criterion_main!(benches);
