# Snapshots

Conceptually, snapshots are straightforward to implement (see MVCC.md). This document discusses the technical challenges involved in threading RowMetadata into the Vector so that it can be used alongside the validity mask.

The main challenge is an OCP violation. We need to thread the row metadata directly into the vector, creating a tight coupling with the table, as the vector can then only be created through the TableAPI. This effectively couples the top-level layer directly to the lowest layer.

The proposed solution needs to satisfy the following requirements:

1) Every entry in the vector must have a corresponding RowMetadata. This is guaranteed structurally: `MutableRowStateChunk` is `!Sync`, and `write` is the only code path that appends to both a data chunk and `RowState`. See Requirement 1 under Solving Requirements.
2) Each windowed view must receive the correctly sliced subsection of the underlying RowMetadata.
3) The process must be zero-copy and preferably zero-cost, as reads are expected to happen very frequently. Note that zero-cost requires monomorphization (`impl Trait` or generics); `dyn Trait` dispatch adds a vtable indirection on the hot read path.

## High-Level Abstraction Choice

The chosen abstraction is an interface, as it allows us to decouple the components. The view/vector owns the interface and defines what it needs from the metadata provider. This also allows us to easily mock metadata for testing purposes.

An alternative would be to introduce a specialized carrier type whose sole purpose is to transport data between these two components. However, this approach offers no clear advantages over the interface-based solution, so we will not pursue it further.

## What Does the Vector Need from the Interface?

The vector primarily needs verified information about the RowMetadata in a form that is fast to consume.

A naive approach — passing a `&[RowMetadata]` slice — does not work here. `RowState` stores metadata as a linked list of separately allocated `Arc<[M; CHUNK_SIZE]>` fixed-size arrays, chained via `OnceLock`. There is no contiguous backing allocation that spans the full column, so a single slice across chunk boundaries cannot be formed without copying.

Additionally, the mutable tail of `RowState` (`MutableRowStateChunk`) holds a `MaybeUninit` array with a fill pointer; only the prefix up to `current` is initialized. This partial, unfinished chunk has no well-typed slice representation until it is frozen.

These two facts directly motivate a state machine. Each state represents one `RowStateChunk` — a full, frozen array of exactly `CHUNK_SIZE` elements. Transitions follow the `next` pointer in the chunk chain. Eventually, we reach a Terminal state corresponding to the mutable tail, which holds the final `k < CHUNK_SIZE` initialized elements. Once those are consumed, no further data is available.

The state machine is not an abstraction for its own sake: it is the natural read-side mirror of the write-side storage structure. It also gives us an explicit model of valid transitions, potentially allowing us to encode the safety guarantees in the type system so that the compiler can enforce them.


## Considering Concurrency

Concurrency is permitted at the chunk level. Frozen chunks are immutable and `Arc`-backed, so multiple threads can safely take snapshots from the same chunk simultaneously without synchronization. Write access remains exclusively owned — `MutableRowStateChunk` is `!Sync` and insert operations take `RowState` by value — ensuring that no snapshot races with an in-progress write.

Every snapshot is tied to the full chunk chain: a snapshot does not represent a point-in-time view of a single chunk in isolation, but of every chunk that exists at the moment it is taken.


## Solving Requirements

## Requirement 1

Rather than checking at runtime that every data row has a corresponding metadata entry, we can show that the inconsistency is structurally unreachable.

The argument rests on two properties:

1. `MutableRowStateChunk` and `Column` is `!Sync`. The compiler therefore prevents any two threads from holding a mutable reference to the same chunk simultaneously — concurrent writes that could interleave a data append with a metadata append are impossible.
2. `write` is the only code path that appends to a data chunk and to `RowState`. It always does both. There is no call site that updates one without the other.

Taken together: enumerate all mutation paths (there is one), show it maintains the invariant (it always writes data and metadata together), and observe that `!Sync` prevents any concurrent execution from breaking that atomicity. The inconsistency has no execution that reaches it.

`TableRow::commit` is that single mutation path:

```rust
fn commit<'table, T: TableRow, M: RowMetadata, F: Fn() -> M>(
    table: &mut Table<T, M>,
    f: F,
    num_elements: LogicalSize,
    mut writes: SmallVec<[PendingWrite; 10]>,
) -> impl Iterator<Item = RowIndex<'table, Self::Schema>> + use<'table, Self, T, M, F>
where
    <Self as TableRow>::Schema: 'table,
{
    while let Some(PendingWrite { index, callback }) = writes.pop() {
        callback(T::get_column_mut(table, index).expect("The Index Resolution is cooked"))
            .expect("Terminating Program: Datastorage has been corrupted. Verify if your index mapping is actually mapping to the right column type.")
    }

    let metadata: RowState<M> = table
        .metadata
        .take()
        .expect("Table is in invalid state. Either concurrent writes are happening or TableBuilder failed");

    let (metadata, index_generator) = metadata.insert_n::<Self::Schema>(num_elements, f);
    table.metadata = Some(metadata);

    index_generator
}
```

The data writes (the `while` loop) and the metadata write (`insert_n`) happen sequentially with no branch between them. There is no early return, no fallible path, and no call site that reaches one without the other.


## Requirement 2

Data and metadata chunks are co-indexed: both use `CHUNK_SIZE`, both fill in the same order, so data chunk `i` always corresponds exactly to metadata chunk `i`. A vector at local offset `k` with length `n` within data chunk `i` can therefore index directly into `metadata_chunk_i[k..k+n]` with no cross-chunk arithmetic. No global row tracking is needed — the state machine provides the correct metadata chunk, and the vector uses its local offset to slice into it.

The co-indexing cannot drift out of sync. Requirement 1 establishes that data and metadata are always written together through the single entry point `TableRow::commit`, with `!Sync` preventing any concurrent interleaving. Every data row appended to chunk `i` is accompanied by a metadata entry appended to metadata chunk `i` in the same operation. The alignment is structural, not assumed.


## Requirement 3

The chunk-based design lends itself exceptionally well to zero-copy snapshots. For frozen chunks, both the data side (`Arc<dyn Array>` in `FrozenChunk`) and the metadata side (`Arc<[M; CHUNK_SIZE]>` in `RowStateChunk`) can be snapshotted by cloning their respective `Arc`s. This is a single atomic reference count increment — no array bytes are copied. Holding the `Arc` also prevents the underlying allocation from being dropped, making use-after-free structurally impossible.

The mutable tail is split. On the metadata side, `MutableRowStateChunk` holds `Arc<[MaybeUninit<M>; CHUNK_SIZE]>` internally — the `Arc` can be cloned directly, just like a frozen chunk. The only additional step is capturing `current` at acquisition time to bound the valid prefix. The `!Sync` is on the wrapper struct, not on the `Arc` itself, so sharing the inner allocation across threads is safe.

On the data side, `MutableChunk` uses `Box<dyn ArrayBuilder>`. Arrow's builder API does not expose its underlying buffer as a shared reference, so a naïve snapshot would call `finish_cloned()`, which allocates a new buffer and copies all current values into it. This is a real `memcpy` of up to `CHUNK_SIZE` rows per column per snapshot.

This limitation is resolved by `ChunkBuffer` and `WriteBufferSnapshot` (see `writer_buffer.md`). Each committed transaction's rows are frozen into an `Arc<dyn Array>` at flush time; snapshots clone those `Arc`s — zero-copy reference-count bumps. The one real copy (`concat`) happens only at publish time when the chunk transitions to frozen.
