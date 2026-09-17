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
| 1,024 | 221.47 ns | 51.674 GiB/s |
| 16,384 | 2.8477 µs | 64.300 GiB/s |
| 65,536 | 10.972 µs | 66.757 GiB/s |
| 131,072 | 22.227 µs | 65.904 GiB/s |
| 1,048,576 | 197.26 µs | 59.406 GiB/s |

#### `arrow_append` — Raw Arrow Builders (Baseline)

Measures only the underlying Arrow `append_slice` calls — no validation, no versioning, no chunk management. This establishes the performance floor to show that the Table abstraction is effectively zero-cost.

| Rows | Time (median) | Throughput (median) |
|------------:|------------------:|------------------------:|
| 1,024 | 296.17 ns | 38.640 GiB/s |
| 16,384 | 2.9335 µs | 62.419 GiB/s |
| 65,536 | 11.107 µs | 65.942 GiB/s |
| 131,072 | 22.094 µs | 66.300 GiB/s |
| 1,048,576 | 193.75 µs | 60.484 GiB/s |

> `bulk_insert` is zero-cost relative to raw Arrow — and outperforms it at N < 2¹⁶, where the mutable tail stays open and finalization is deferred.
