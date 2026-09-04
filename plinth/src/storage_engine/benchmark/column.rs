use std::{
    hint::black_box,
    sync::{Arc, OnceLock, atomic::Ordering},
};

use arrow::array::{Array, ArrayBuilder, Int32Builder};
use criterion::{BenchmarkId, Criterion, Throughput};
use std::sync::atomic::AtomicU64;

use crate::{
    chunk::{ChunkWriter, MutableChunk},
    storage_engine::{chunk::CHUNK_SIZE, column::Column, units::VersionID},
};

fn version_generator() -> Box<dyn Fn() -> VersionID> {
    let next_id: AtomicU64 = AtomicU64::new(0);

    Box::new(move || VersionID::new(next_id.fetch_add(1, Ordering::Relaxed)))
}

fn make_column<B: ArrayBuilder>(builder: B) -> Column {
    Column::new::<B>(version_generator(), builder)
}

pub fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_append");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size as u64));

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

pub fn bench_chunk_writer_64k(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_writer");

    group.throughput(Throughput::Elements(CHUNK_SIZE.get() as u64));

    group.bench_function("65536", |b| {
        b.iter(|| {
            let mutable_chunk = MutableChunk::new(Box::new(Int32Builder::new()), VersionID::new(0));

            let mut writer = unsafe { mutable_chunk.builder::<Int32Builder>().unwrap_unchecked() };

            for value in 0..CHUNK_SIZE.get() as i32 - 1 {
                writer = writer.append(black_box(value)).unwrap();
            }

            black_box(writer);
        });
    });

    group.finish();
}

pub fn bench_chunk_writer_64k_roll_over(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_writer");

    group.throughput(Throughput::Elements(CHUNK_SIZE.get() as u64));
    group.bench_function("65536_rollover", |b| {
        b.iter(|| {
            let tail: OnceLock<Arc<dyn Array>> = OnceLock::new();

            let mutable_chunk = MutableChunk::new(Box::new(Int32Builder::new()), VersionID::new(0));

            let mut writer = unsafe { mutable_chunk.builder::<Int32Builder>().unwrap_unchecked() };

            for value in 0..CHUNK_SIZE.get() as i32 {
                match writer.append(black_box(value)) {
                    Ok(old_writer) => {
                        writer = old_writer;
                    }
                    Err((mut builder, _id)) => {
                        let array: Arc<dyn Array> = Arc::new(builder.finish());

                        unsafe { tail.set(array).unwrap_unchecked() }
                        black_box(tail);
                        return;
                    }
                }
            }

            black_box(writer);
        });
    });

    group.finish();
}

pub fn bench_column_write_body(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_write_body");
    group.throughput(Throughput::Elements(CHUNK_SIZE.get() as u64));

    group.bench_function("65536", |b| {
        b.iter(|| {
            let tail = MutableChunk::new(Box::new(Int32Builder::new()), VersionID::new(0));

            let mut writer: ChunkWriter<Int32Builder> =
                unsafe { tail.builder().unwrap_unchecked() };

            let values = 0..CHUNK_SIZE.get() as i32 - 1;

            for value in values {
                match writer.append(value) {
                    Ok(new_builder) => {
                        writer = new_builder;
                    }
                    Err(_) => unreachable!(),
                }
            }

            black_box(MutableChunk::from(writer));
        });
    });

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
        group.throughput(Throughput::Elements(size as u64));

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

pub fn bench_raw_arrow_chunked(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw_arrow_chunked_append");

    for size in [
        1_024,
        16_384,
        CHUNK_SIZE.get(),
        (CHUNK_SIZE + CHUNK_SIZE).get(),
        1_048_576,
    ] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut remaining = size;

                while remaining > 0 {
                    let chunk_size = remaining.min(65_536);
                    let mut builder = Int32Builder::new();

                    for value in 0..chunk_size as i32 {
                        builder.append_value(black_box(value));
                    }

                    black_box(builder.finish());

                    remaining -= chunk_size;
                }
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
        group.throughput(Throughput::Elements(size as u64));

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
