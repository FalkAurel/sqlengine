---
title: "I Worked on SAP's SQL Engine. Then I Went Home and Built My Own."
date: 2026-09-13
excerpt: "This summer I interned at SAP's ABAP SQL Kernel team, extended a production SQL engine in C++, and cut query execution time by 75%. My supervisor told me my abilities far exceeded expectations. So I went home and started building my own storage engine in Rust — bottom-up, layer by layer. This is the first post in that series."
---

This summer I interned at SAP's ABAP SQL Kernel team. I spent three months extending a production SQL engine in C++ — adding support for non-trivial `GROUP BY` operands, `HAVING` filters, `DISTINCT COUNT`, and common subexpression elimination. By the end, some query paths were 75% faster.

My supervisor's written feedback included: *"Knowledge and skills far exceed expectations for a dual-study student in their first practical phase."*

That was the push I needed. If the ceiling is that far away, I want to find out where it is.

I'm 21. I'm building a storage engine in Rust from scratch — and when it's done, I'm going to build a full ISO-compliant SQL engine on top of it. This is the first post in that series.

---

## The Plan

The stack I'm building, bottom up:

```text
SQL Engine          ← future
     │
  Table API         ← done
     │
Storage Engine      ← this post
     │
 Apache Arrow       ← backing format
```

Each layer has a design document before a line of code is written. The design documents live alongside the implementation in the repo. This post is about the storage engine layer.

---

## Why Apache Arrow as the Backing Format?

Arrow is a columnar in-memory format with zero-copy reads, SIMD-friendly layout, and a mature Rust implementation. Using it as the physical layer means I'm not reinventing buffer management — I'm building above it.

The relevant comparison is what DuckDB does. DuckDB uses Arrow-compatible columnar segments internally, which means full memory segments are never mutated. Readers can access data without synchronizing with writers. I want the same property, and starting from Arrow gets me there without having to design the memory layout myself.

The tradeoff is that I'm now at the mercy of Arrow's builder API, which turns out to matter more than I expected. More on that shortly.

---

## The Architecture

The storage layer is a three-level hierarchy:

```text
Column
  │
  ├── head: Option<Arc<FrozenChunk>>
  ├── frozen_tail: Option<Arc<FrozenChunk>>
  └── tail: Option<MutableChunk>
                    │
                    ▼
             ChunkWriter<B>
                    │
                    ▼
             Arrow ArrayBuilder
```

A `Column` owns a mutable tail chunk. When the tail fills (at 65,536 values), it's frozen — converted from a mutable Arrow builder into an immutable `Arc<dyn Array>` — and linked into the chain. New writes go into the next tail chunk.

The frozen representation gives you the same guarantee DuckDB has: published data is immutable. A reader holding a reference to a frozen chunk can iterate it indefinitely without any coordination with a writer appending to the tail.

The `FrozenChunk` stores an `Arc<dyn Array>`, so freezing does not copy data. It re-wraps what the Arrow builder already owns:

```rust
let array: Arc<dyn Array> = Arc::new(builder.finish());
let frozen = Arc::new(FrozenChunk::new(array, chunk_id));
```

That's the only allocation at rollover time. The per-element path touches nothing outside the builder.

---

## The 2× Slowdown I Did Not Expect

Once I had a working `Column`, I benchmarked it against writing directly through a `ChunkWriter`. The result was surprising:

```text
Direct ChunkWriter       ~1.1B elements/sec
Column::write            ~530M elements/sec
```

Roughly half the throughput. The `Column` abstraction looked lightweight — fully generic, statically dispatched, no `dyn` in the hot path. I expected the compiler to flatten it.

It mostly did. But not completely.

### Ruling Out the Obvious Suspects

I worked through the plausible explanations one by one:

| Suspect | Result |
|---|---|
| `Box<dyn Fn() -> VersionID>` for ID generation | Replaced with static atomic. No improvement. |
| `Arc` reference counting | `Arc::clone` happens at rollover, not per element. Not the issue. |
| Chunk rollover | Isolated test showed ~2% difference. Not the issue. |
| `impl Iterator` parameter | Static dispatch. Monomorphized. Not inherently expensive. |
| Per-element `black_box` in benchmark | Real ~10% artifact. Not the full gap. |
| `#[inline(always)]` on `Column::write` | ~1–2% improvement. Not the issue. |

None of those explained 2×. So I looked at the assembly.

### Reading the Machine Code

```bash
cargo build --release --features bench --bin profile_column
nm -C -S target/release/profile_column | grep 'Column.*write'
objdump -dC target/release/profile_column
```

The hot loop in `Column::write` contained this:

```asm
cmpq   $0x10000,0x10(%rbx)    ; check builder length < CHUNK_SIZE
jae    ...
mov    0x38(%rsp),%esi         ; load value
mov    %rbx,%rdi
call   c50a0                   ; CALL Append::append
cmpq   $0x10000,0x10(%rbx)    ; check builder length again
jb     loop
```

There's a real `call` instruction inside the per-element loop. The target was:

```text
<PrimitiveBuilder<Int32Type> as plinth::...::Append>::append
```

`ChunkWriter::append` had been inlined — there was no call to it. But the one step further down, the concrete `Append` implementation for `PrimitiveBuilder`, was not. Every element paid a full function call.

At ~530M elements/sec, that's hundreds of millions of calls per second. The profiler agreed:

```text
59.67%  <PrimitiveBuilder<Int32Type> as Append>::append
```

Source, profiler, and disassembly all pointed at the same place.

### Why the Compiler Wouldn't Inline It

The `Append` trait implementation sat behind an `Option<Box<B>>`:

```rust
struct MutableChunk {
    builder: Option<Box<dyn AppendableType>>,
}
```

Even in the generic, statically-dispatched path, the `Option<Box<B>>` wrapper forced the compiler to emit a null check on every iteration. That check blocked inlining because the compiler could not prove the pointer was always valid — even though the logic guaranteed it was.

The fix was to change the representation to `Box<B>` directly, giving the compiler the invariant that the pointer is always non-null:

```rust
struct ChunkWriter<B> {
    builder: Box<B>,
    // ...
}
```

And mark the `Append` implementation:

```rust
#[inline(always)]
fn append(&mut self, value: i32) {
    self.append_value(value);
}
```

After the change, the `call c50a0` instruction disappeared from the hot loop. The assembly became what it should have been from the start: a tight sequence of inlined Arrow builder operations inside the iteration. The throughput closed the gap to near-parity with the direct `ChunkWriter` path.

### The Lesson

The lesson is not "traits are slow" or "generics are slow." `ChunkWriter::append` was inlined. The entire `Column` abstraction collapsed correctly. One boundary survived — and it did so because of a subtle type-level representation that blocked the compiler's proof obligations.

> Source-level reasoning tells you what *should* happen. Disassembly tells you what *actually* happened.

If you're writing performance-sensitive Rust and something is slower than it should be: profile with `perf`, find the hot symbol, read the machine code. The answer is usually precise and fixable. The debugging workflow that found this:

```text
Benchmark → isolate components → remove benchmark artifacts →
perf/flamegraph → suspicious symbol → nm + objdump →
match assembly to source → one targeted change → re-measure
```

---

## Where It Stands

Current benchmark against raw Arrow primitives:

```text
Column::write         ~1.05B elements/sec
Raw Arrow append      ~1.1B elements/sec
```

Near-parity. The `Column` abstraction — with its frozen chunk chain, version IDs, and rollover logic — costs almost nothing compared to writing directly into an Arrow builder.

---

## What's Next

The storage engine layer is the foundation. The next post covers the **Table API**: a type-level schema system where the row type `T` describes its fields statically, and `Table<T>` owns all relational operations. No per-row heap allocation. No intermediate field collections. The schema is derived from `T`'s type, not from runtime introspection.

After that: snapshots, then MVCC — the layer I designed first, before I realized I needed the foundation underneath it first.

The repo is at [github.com/FalkAurel/sqlengine](https://github.com/FalkAurel/sqlengine). The design documents live alongside the code.
