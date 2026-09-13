---
title: "I Worked on SAP's SQL Engine. Then I Went Home and Built My Own."
date: 2026-09-13
excerpt: "This summer I interned at SAP's ABAP SQL Kernel team, extended a production SQL engine in C++, and cut query execution time by 75%. My supervisor told me my abilities far exceeded expectations. So I went home and started building my own storage engine in Rust — bottom-up, layer by layer. This is the first post in that series."
---

This summer I interned at SAP's ABAP SQL Kernel team. Three months in a production C++ codebase, extending the SQL engine: `GROUP BY` already existed for raw columns, but I extended it to work on arbitrary expressions like `(A+B)+(C+D)`. I also implemented `HAVING`, `DISTINCT`, `DISTINCT COUNT`, and common subexpression elimination from scratch. Queries that previously had to be pushed down to HANA could now execute entirely on the application server — up to 75% faster as a result. My supervisor was impressed enough to offer to take me on for my bachelor thesis.

That part was great. What wasn't great was the codebase.

Legacy C++, function macros that made you question your life choices, decades of accumulated decisions that made sense in isolation but compounded into something you had to fight every time you wanted to add something clean. I don't say this to be ungrateful — the work was genuinely interesting and the team was excellent. But by the end of the summer I had a very clear thought: I know what this should look like. I can build it better. In Rust, from scratch, without the legacy baggage.

I'm 21. I've read the Codd paper, the Garcia-Molina textbook, the concurrency literature. I had spent a summer inside a real SQL kernel. So that's what I'm doing — and fair warning, I'm not going to skip the parts where I was debugging at 2am wondering if I'd made a fundamental mistake.

Storage engine first. Then snapshots. Then MVCC. Then a full ISO-compliant SQL engine on top. This is the first post in that series.

---

## The Stack

Bottom up:

```text
SQL Engine          ← future
     │
  Table API         ← done
     │
Storage Engine      ← this post
     │
 Apache Arrow       ← backing format
```

Each layer gets a design document before a line of code is written. That habit came from SAP — the feedback specifically mentioned architectural thinking — and it already saved me from a significant mistake I'll get to at the end of this post. The design docs live in the repo alongside the code.

---

## Why Arrow?

I didn't want to design a memory layout. Arrow gives you a columnar in-memory format with zero-copy reads, SIMD-friendly structure, and a mature Rust implementation. Building above it means I start with buffer management solved and can focus on what's actually interesting.

The model I was targeting is what DuckDB does: full memory segments are never mutated, so readers can access data without synchronizing with writers at all. Arrow gets me that property without designing it from scratch.

The tradeoff — and I knew this going in — is that I'm now coupled to Arrow's builder API. That coupling turned out to matter more than I expected. We'll get there.

---

## The Architecture

Three levels:

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

A `Column` owns a mutable tail chunk. When it fills at 65,536 values, it's frozen — the Arrow builder is finished into an `Arc<dyn Array>` and linked into the chain. Writes continue into a fresh tail.

Freezing doesn't copy anything. The builder already owns the memory; we just re-wrap it:

```rust
let array: Arc<dyn Array> = Arc::new(builder.finish());
let frozen = Arc::new(FrozenChunk::new(array, chunk_id));
```

One allocation at rollover. Nothing in the per-element path. And once a chunk is frozen, it's immutable forever — a reader holding a reference to it doesn't need to coordinate with anything.

That last property is the whole point. It's what makes the MVCC design tractable. But I'm getting ahead of myself.

---

## The 2× Slowdown, and Two Hours of Debugging at 2am

Once the `Column` was working, I benchmarked it:

```text
Direct ChunkWriter       ~1.1B elements/sec
Column::write            ~530M elements/sec
```

Half the throughput. I stared at this for a long time.

The code looked right. Fully generic, statically dispatched, no `dyn` anywhere in the hot path. The whole point of Rust's monomorphization is that the compiler flattens this kind of layered abstraction into one tight loop. That's what I was counting on. That's what I would have bet on.

I was wrong. And I did not figure out why until around 2am.

My first instinct — and this is where my expectations started working against me — was to look for `dyn`. Fat pointer, dynamic dispatch, vtable jump: that's the classic "this is slow" story in Rust. So I looked everywhere I was using `dyn`. I had a `Box<dyn Fn() -> VersionID>` for generating version IDs. I replaced it with a static atomic. No improvement. I looked at `Arc<dyn Array>` in the frozen chunks. That's only touched at rollover, once every 65,536 elements. Not the issue. I looked at the `impl Iterator` parameter. Static dispatch. Monomorphized. Not inherently expensive.

Nothing. I was chasing fat pointers and there weren't any in the hot path. I had completely misidentified the category of problem.

Eventually I stopped guessing and read the assembly.

### What objdump Showed

```bash
cargo build --release --features bench --bin profile_column
nm -C -S target/release/profile_column | grep 'Column.*write'
objdump -dC target/release/profile_column
```

The hot loop:

```asm
cmpq   $0x10000,0x10(%rbx)    ; check builder length < CHUNK_SIZE
jae    ...
mov    0x38(%rsp),%esi
mov    %rbx,%rdi
call   c50a0                   ; <PrimitiveBuilder<Int32Type> as Append>::append
cmpq   $0x10000,0x10(%rbx)    ; check builder length again
jb     loop
```

There's a `call` instruction. Right there, inside the per-element loop. Every single element is paying a function call.

The profiler had been pointing at exactly this all along:

```text
59.67%  <PrimitiveBuilder<Int32Type> as Append>::append
```

I'd been reading that as "the append implementation is slow." It wasn't. It was that the append implementation was never being inlined — so every element crossed a function call boundary, and at 500M+ elements per second, that boundary is expensive.

`ChunkWriter::append` had been inlined. The compiler did collapse that layer. But one level deeper — the concrete `Append` implementation for `PrimitiveBuilder` — survived as a real call. The abstraction mostly collapsed. One boundary didn't.

### The Actual Problem

In hindsight, embarrassingly obvious. I was looking for `dyn` and fat pointers. The real issue was `Option<Box<B>>`.

The builder lived behind an `Option`:

```rust
struct MutableChunk {
    builder: Option<Box<dyn AppendableType>>,
}
```

Even in the fully generic, statically-dispatched path, `Option<Box<B>>` means the compiler has to check on every iteration whether the pointer is null. It can't prove it's always valid — even though the logic guarantees it is — so it emits the check, and that check blocks inlining.

I was hunting vtables. The problem was an innocuous `Option` wrapper that broke the compiler's proof obligations.

The fix: change the representation to `Box<B>` directly, so the compiler gets the non-null invariant for free:

```rust
struct ChunkWriter<B> {
    builder: Box<B>,
}
```

And mark the `Append` implementation for inlining:

```rust
#[inline(always)]
fn append(&mut self, value: i32) {
    self.append_value(value);
}
```

Rebuilt. Checked the assembly. The `call c50a0` was gone. The loop became a tight sequence of inlined Arrow builder operations, exactly what it should have been from the start.

### Where It Stands Now

```text
Column::write         ~1.05B elements/sec
Raw Arrow append      ~1.1B elements/sec
```

Near-parity. The full `Column` abstraction — frozen chunk chain, version IDs, rollover — costs essentially nothing over writing directly into an Arrow builder.

### What This Actually Taught Me

Not "traits are slow." Not "generics are slow." Those would be wrong lessons. The abstraction was fine. Monomorphization worked. The compiler collapsed almost everything.

The problem was a representation detail I hadn't thought through, and I spent hours not finding it because I was looking for the wrong thing. I knew what Rust performance problems looked like — fat pointers, dynamic dispatch, missed inlining on `dyn` — and I kept looking for that pattern even after the evidence stopped supporting it.

> Your mental model of what's slow will blind you to what's actually slow.

When you're stuck: stop reasoning from the source. Profile, find the hot symbol, read the machine code. The truth is in there and it's usually precise.

---

## The Design Mistake I Caught Before It Cost Me Any Code

One more thing, because it's relevant to how this project is structured.

I started the design phase at the top of the stack — with MVCC. That's where the interesting problems are: snapshot isolation, concurrent readers and writers, version visibility. I wrote the whole design document. Then I looked at it and realized I had nothing to implement it against. The design was correct in the abstract but had no foundation.

So I stopped and went down. Storage engine first. Table API second. Now I'm back to MVCC with actual primitives to reason about.

That wasn't an accident — it was the same instinct from SAP. Design before you code, and interrogate the design before you commit to it. The MVCC document is deliberately vague: no code, no data structures, just invariants and protocol. It could afford to be vague because it was never going to be implemented until the layers below it existed.

The mistake was misjudging the scope. The catch was catching it in the design phase, which cost nothing, instead of in the implementation phase, which would have cost a lot.

---

## What's Next

The next post covers the **Table API**: a type-level schema where `T` describes its fields statically, `Table<T>` owns all relational operations, and insertion requires zero per-row heap allocation. No intermediate field collections. The schema is derived from `T`'s type at compile time.

After that: snapshots, then MVCC.

The repo is at [github.com/FalkAurel/sqlengine](https://github.com/FalkAurel/sqlengine). The design documents are in `design-docs/`.
