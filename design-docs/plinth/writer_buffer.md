# ChunkBuffer & Zero-Copy Mutable Snapshots

This document describes the `writer_buffer` module: the `ChunkBuffer` staging type, the `WriteBufferSnapshot` snapshot type, and the lazy-flush design that together eliminate the `memcpy` from the mutable chunk's read path.

See `snapshot.md` for the overall snapshot design and the known limitation this module resolves.

---

## Problem

`snapshot.md` (Requirement 3) documents a real `memcpy` on the mutable data tail:

> Arrow's builder API does not expose its underlying buffer as a shared reference, so `get_snapshot()` calls `finish_cloned()`, which allocates a new buffer and copies all current values into it.

Every call to `read_mutable()` — before every read of the in-flight chunk — triggered an O(n) allocation and copy of up to `CHUNK_SIZE` rows. On a column with 64-bit values this was up to 512 KB per snapshot call.

The fix cannot be "expose the builder's buffer directly" because Arrow intentionally does not provide that. The fix is to stop using the builder as the snapshot source.

---

## Solution: ChunkBuffer

`ChunkBuffer` is a per-`MutableChunk` staging buffer. Instead of reading from the still-growing builder at snapshot time, each committed transaction's rows are frozen into an `Arc<dyn Array>` at write time. Snapshots clone those `Arc`s — zero-copy reference-count bumps.

The one real copy (`concat` in `fuse_arrays`) happens exactly once per chunk, at publish time, when the chunk transitions from mutable to frozen. This is unavoidable and was always paid implicitly; the new design makes it explicit and moves it off the read path.

### Invariant

```
builder.len() + chunk_buffer.size() <= CHUNK_SIZE
```

This is enforced at the entry point — `make_builder` rejects any builder that would overflow — so no call site needs to reason about overflow independently.

### State machine per write call

```
write(rows) {
    append rows to builder                  // builder accumulates
    
    if builder + buffer would exceed CHUNK_SIZE:
        Err((builder, chunk_buffer, chunk_id))  // caller handles overflow
    else:
        // stored back into MutableChunk via into_parts / from_raw
}
```

On the `Err` path (chunk fills mid-write), the caller runs `fuse_arrays` to produce one contiguous `Arc<dyn Array>` for the `FrozenChunk`, then starts a fresh `MutableChunk`.

---

## Lazy Flush

An earlier design flushed the builder at the end of every `write()` call, producing one `Arc<dyn Array>` per write. For workloads that insert one row at a time, a full chunk of 65,536 rows would create 65,536 separate allocations.

**The current design is lazy:** the builder accumulates rows across arbitrarily many `write()` calls. The flush happens only in `read_mutable()`, which calls `builder.finish()` (a zero-copy `Vec → Buffer` transfer) and commits the result to `ChunkBuffer`.

```
read_mutable() {
    if builder has pending rows:
        flush builder → Arc<dyn Array> → push to chunk_buffer.arrays
    return chunk_buffer.snapshot()          // Arc::clone per accumulated array
}
```

Between two snapshot calls: exactly one `Arc<dyn Array>` is created, regardless of how many `write()` calls happened in between.

---

## WriteBufferSnapshot

```rust
pub(crate) struct WriteBufferSnapshot(Box<[Arc<dyn Array>]>);
```

`WriteBufferSnapshot` holds owned `Arc`s — one per committed transaction since the last snapshot. Acquiring a snapshot is `Arc::clone` per array, which is a few atomic increments and no data movement.

The snapshot is independent of the originating `ChunkBuffer`. Writing more rows after `read_mutable()` returns does not affect any snapshot already taken. The snapshot remains valid for as long as the caller holds it, even if the source `MutableChunk` is published or dropped.

### Iteration

```rust
// Raw per-commit arrays
snapshot.iter() -> impl Iterator<Item = &Arc<dyn Array>>

// Typed iteration — yields VectorIter<A> per committed array
snapshot.vectors::<A>() -> impl Iterator<Item = VectorIter<'_, A>>
```

Both are zero-copy. There is no `into_single_array()` method — callers work with per-commit arrays directly or use `fuse_arrays()` via `FrozenChunk` if they need a contiguous buffer.

---

## Freeze / Publish Path

`FrozenChunk` must always contain exactly one contiguous `Arc<dyn Array>` of exactly `CHUNK_SIZE` rows. This is enforced by `fuse_arrays()`, called exactly once at publish time:

- **Single-array case** (all rows came from one builder flush): `arrays[0].clone()` — no copy.
- **Multi-array case** (multiple write calls flushed before the chunk filled): `concat(&refs)` — one allocation of exactly `CHUNK_SIZE` rows.

`concat` is safe here because all arrays in a `ChunkBuffer` come from the same concrete builder type. Arrow's `concat` requires homogeneous types; the type invariant is maintained structurally by `ChunkWriter<B>`, which is generic over a single `B: Append`.

---

## Safety

`ChunkBuffer` is `!Sync` via `PhantomData<*const ()>`. Ownership transfers between `MutableChunk` and `ChunkWriter`; there is never more than one owner at a time. No synchronization primitive is needed.

```rust
unsafe impl Send for ChunkBuffer {}
```

`Send` is safe: `ChunkBuffer` contains `Vec<Arc<dyn Array>>`. `Arc<dyn Array>` is `Send + Sync` (Arrow guarantees this). The raw-pointer marker affects only `Sync`. Sending `ChunkBuffer` across a thread boundary is sound.

---

## Performance Summary

| Operation               | Old design            | New design             |
|-------------------------|-----------------------|------------------------|
| `write()` per call      | flush builder → Arc   | nothing (accumulate)   |
| `read_mutable()`        | `finish_cloned()` + alloc | flush if pending, clone Arcs |
| Publish (`freeze`)      | `concat` once         | `concat` once (unchanged) |
| Snapshot cost           | O(n) copy             | O(k) where k = committed arrays since last snapshot |

For a typical workload that writes a full chunk before reading, k = 1 and the snapshot is a single `Arc::clone`.
