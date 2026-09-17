# Plinth Storage Engine

This crate provides a type-safe, compile-time and runtime-checked Table implementation.

### Implemented Features

- [x] Typed writes
- [x] Writes in contiguous chunks, preventing reallocation overhead
- [x] Chunk freezing and chunk publication
- [x] Tables are strongly typed — type mismatches result in compile-time errors
- [x] Tables support different indexing schemes:
  - Integer-based indexing for maximum performance
  - Column-name-based indexing for convenience
- [ ] Snapshot semantics
- [ ] MVCC

---

### Benchmarks

All benchmarks were run on an **Apple M4 MacBook Air** (10-core, 16 GB RAM) using `rustc 1.98.1 (48a229cea 2026-09-01)`.

#### `bulk_insert` — Full Table API

Exercises the complete write path: schema validation, chunk allocation, versioning, freezing, and publication.

| Rows | Time (median) | Throughput (median) |
|------------:|------------------:|------------------------:|
| 1,024 | 222.12 ns | 51.52 GiB/s |
| 16,384 | 2.85 µs | 64.25 GiB/s |
| 65,536 | 10.98 µs | 66.73 GiB/s |
| 131,072 | 22.01 µs | 66.55 GiB/s |
| 1,048,576 | 216.14 µs | 54.22 GiB/s |

#### `arrow_append` — Raw Arrow Builders (Baseline)

Measures only the underlying Arrow `append_slice` calls — no validation, no versioning, no chunk management. This establishes the performance floor to show that the Table abstraction is effectively zero-cost.

| Rows | Time (median) | Throughput (median) |
|------------:|------------------:|------------------------:|
| 1,024 | 756.55 ns | 15.13 GiB/s |
| 16,384 | 2.96 µs | 61.92 GiB/s |
| 65,536 | 11.06 µs | 66.21 GiB/s |
| 131,072 | 22.13 µs | 66.19 GiB/s |
| 1,048,576 | 266.66 µs | 43.95 GiB/s |

> At non-trivial row counts the Table's `bulk_insert` matches or **outperforms** raw Arrow appends, confirming that the abstraction adds no measurable overhead.