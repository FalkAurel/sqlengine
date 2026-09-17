use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use plinth::{
    table::{Empty, Node, SliceTableRow, Table, TableBuilder, TableRow},
    units::LogicalSize,
};
use std::mem::size_of;

struct UserSlices<const N: usize> {
    ids: Box<[i32; N]>,
    values: Box<[i64; N]>,
}

impl<const N: usize> TableRow for UserSlices<N> {
    type Schema = Node<i64, Node<i32, Empty>>;
}

impl<const N: usize> SliceTableRow for UserSlices<N> {
    fn visit_columns_slice<'a, V: plinth::table::SliceFieldVisitor<'a>>(
        &'a self,
        visitor: &mut V,
    ) -> Result<(), plinth::table::VisitorError> {
        visitor.visit_slice::<N, usize, i32>(0, &self.ids)?;
        visitor.visit_slice::<N, usize, i64>(1, &self.values)
    }
}

fn benchmark_mass_api_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");

    macro_rules! bench {
        ($n:expr) => {{
            const N: usize = $n;

            let bytes = (N * (size_of::<i32>() + size_of::<i64>())) as u64;

            group.throughput(Throughput::Bytes(bytes));

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

            let input = UserSlices::<N> { ids, values };

            group.bench_function(BenchmarkId::from_parameter(N), |b| {
                b.iter_batched_ref(
                    || {
                        TableBuilder::default()
                            .add::<i32>("ids")
                            .unwrap()
                            .add::<i64>("values")
                            .unwrap()
                            .finish::<UserSlices<N>>()
                    },
                    |table| {
                        std::hint::black_box(table.bulk_insert(&input).unwrap());
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }};
    }

    bench!(1024);
    bench!(16_384);
    bench!(65_536);
    bench!(131_072);
    bench!(1_048_576);

    group.finish();
}

fn benchmark_arrow_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_append");

    macro_rules! bench {
        ($n:expr) => {{
            const N: usize = $n;
            const CHUNK_SIZE: usize = 64 * 1024;

            let bytes = (N * (size_of::<i32>() + size_of::<i64>())) as u64;
            group.throughput(Throughput::Bytes(bytes));

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

            group.bench_function(BenchmarkId::from_parameter(N), |b| {
                b.iter(|| {
                    let mut i32_builder = arrow::array::Int32Builder::with_capacity(N);
                    let mut i64_builder = arrow::array::Int64Builder::with_capacity(N);

                    for start in (0..N).step_by(CHUNK_SIZE) {
                        let end = (start + CHUNK_SIZE).min(N);

                        std::hint::black_box(
                            i32_builder.append_slice(std::hint::black_box(&ids[start..end])),
                        );

                        std::hint::black_box(
                            i64_builder.append_slice(std::hint::black_box(&values[start..end])),
                        );
                    }

                    std::hint::black_box(i32_builder.finish());
                    std::hint::black_box(i64_builder.finish());
                });
            });
        }};
    }

    bench!(1024);
    bench!(16_384);
    bench!(65_536);
    bench!(131_072);
    bench!(1_048_576);

    group.finish();
}

criterion_group!(
    benches,
    benchmark_mass_api_insertion,
    benchmark_arrow_append
);
criterion_main!(benches);
