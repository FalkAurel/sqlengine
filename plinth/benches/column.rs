// benches/column.rs

use criterion::{criterion_group, criterion_main};
use plinth::benchmark::{
    bench_column_creation, bench_column_write,
    bench_column_write_values, bench_raw_arrow, bench_raw_arrow_chunked_retained,
};

criterion_group!(
    benches,
    bench_column_write,
    bench_column_write_values,
    bench_column_creation,
    bench_raw_arrow,
    bench_raw_arrow_chunked_retained
);
criterion_main!(benches);
