# sqlengine / plinth

[![CI](https://github.com/FalkAurel/sqlengine/actions/workflows/ci.yml/badge.svg)](https://github.com/FalkAurel/sqlengine/actions/workflows/ci.yml)
[![Benchmarks](https://github.com/FalkAurel/sqlengine/actions/workflows/benchmark.yml/badge.svg)](https://FalkAurel.github.io/sqlengine/dev/bench/)

Storage engine built on Apache Arrow.

## Benchmarks

Historical benchmark results are tracked automatically on every push to `main` and rendered as an interactive chart:

**[View benchmark dashboard →](https://FalkAurel.github.io/sqlengine/dev/bench/)**

The chart plots throughput over time for each benchmark group:

| Group | What it measures |
|---|---|
| `column_write` | Writing a stream of values into a `Column` at various sizes |
| `column_write_values` | Writing a pre-allocated slice into a `Column` |
| `chunk_writer_append` | Single-value append path through `ChunkWriter` |
| `column_creation` | Baseline cost of constructing a `Column` |
| `raw_arrow_append` | Raw Arrow `Int32Builder` append (baseline comparison) |
| `raw_arrow_chunked_retained` | Chunked Arrow append with retained arrays |

A regression alert is posted as a comment on the triggering commit whenever any benchmark exceeds 150% of its stored baseline (i.e. becomes >50% slower).

### Running benchmarks locally

```sh
cargo bench --features bench
```

> **Note:** Local results use `target-cpu=native` (see `.cargo/config.toml`) and will differ from CI numbers, which run on a generic CPU. Use local numbers for absolute performance and CI numbers for trend tracking.

## Development

```sh
cargo test --all-features   # run all tests
cargo clippy --all-targets  # lint
cargo fmt                   # format
```

### One-time GitHub Pages setup

After the first successful `Benchmark` workflow run, enable GitHub Pages in
**Settings → Pages → Source → Deploy from a branch → `gh-pages` / `/ (root)`**
so the dashboard URL above becomes live.
