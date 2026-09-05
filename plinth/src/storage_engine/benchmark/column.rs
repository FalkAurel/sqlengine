use std::{hint::black_box, sync::atomic::Ordering};

use arrow::array::{ArrayBuilder, Int32Builder};
use criterion::{BenchmarkId, Criterion, Throughput};
use std::sync::atomic::AtomicU64;

use crate::{
    storage_engine::{chunk::CHUNK_SIZE, column::Column},
    units::VersionID,
};

fn version_generator() -> Box<dyn Fn() -> VersionID> {
    let next_id: AtomicU64 = AtomicU64::new(0);

    Box::new(move || VersionID::new(next_id.fetch_add(1, Ordering::Relaxed)))
}

fn make_column<B: ArrayBuilder>(builder: B) -> Column {
    Column::new::<B>(version_generator(), builder)
}

pub fn bench_column_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_write");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut column: Column = make_column(Int32Builder::new());

                let values = (0..size as i32).map(black_box);

                column.write::<Int32Builder>(values).unwrap();

                black_box(column);
            });
        });
    }

    group.finish();
}

pub fn bench_column_write_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_write_values");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let values: Vec<i32> = (0..size as i32).collect();
            b.iter(|| {
                let mut column: Column = make_column(Int32Builder::new());
                column
                    .write_values::<Int32Builder>(black_box(&values))
                    .unwrap();
                black_box(column);
            });
        });
    }

    group.finish();
}

pub fn bench_column_creation(c: &mut Criterion) {
    c.bench_function("column_creation", |b| {
        b.iter(|| {
            black_box(make_column(Int32Builder::new()));
        });
    });
}

pub fn bench_raw_arrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw_arrow_append");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut builder = Int32Builder::new();

                for value in 0..size as i32 {
                    builder.append_value(black_box(value));
                }

                black_box(builder);
            });
        });
    }

    group.finish();
}

pub fn bench_raw_arrow_chunked_retained(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw_arrow_chunked_retained");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut arrays = Vec::new();
                let mut remaining = size;

                while remaining > 0 {
                    let chunk_size = remaining.min(CHUNK_SIZE.get());
                    let mut builder = Int32Builder::new();

                    for value in 0..chunk_size as i32 {
                        builder.append_value(black_box(value));
                    }

                    arrays.push(builder.finish());

                    remaining -= chunk_size;
                }

                black_box(arrays);
            });
        });
    }

    group.finish();
}

pub fn profile_append_1m() {
    let mut column = make_column(Int32Builder::new());

    let values = (0..1_048_576i32).map(std::hint::black_box);

    column.write::<Int32Builder>(values).unwrap();

    std::hint::black_box(column);
}

pub fn profile_append_64k() {
    let mut column = make_column(Int32Builder::new());

    let values = (0..65_536i32).map(std::hint::black_box);

    column.write::<Int32Builder>(values).unwrap();

    std::hint::black_box(column);
}
