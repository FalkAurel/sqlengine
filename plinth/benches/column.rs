// benches/column.rs

use criterion::{criterion_group, criterion_main};
use plinth::benchmark::{bench_append, bench_column_creation, bench_raw_arrow, bench_raw_arrow_chunked, bench_raw_arrow_chunked_retained};

criterion_group!(benches, bench_append, bench_column_creation, bench_raw_arrow, bench_raw_arrow_chunked, bench_raw_arrow_chunked_retained);
criterion_main!(benches);