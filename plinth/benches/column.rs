// benches/column.rs

use criterion::{criterion_group, criterion_main};
use plinth::benchmark::{
    bench_append, bench_chunk_writer_64k, bench_chunk_writer_64k_roll_over, bench_column_creation,
    bench_column_write_body, bench_raw_arrow, bench_raw_arrow_chunked,
    bench_raw_arrow_chunked_retained,
};

criterion_group!(
    benches,
    bench_append,
    bench_column_write_body,
    bench_chunk_writer_64k,
    bench_chunk_writer_64k_roll_over,
    bench_column_creation,
    bench_raw_arrow,
    bench_raw_arrow_chunked,
    bench_raw_arrow_chunked_retained
);
criterion_main!(benches);
