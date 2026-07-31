# Plinth Storage Engine Design Document

## Overview

Plinth is the foundational storage layer for a SQL engine. It provides an efficient, column-oriented, in-memory data model with persistence, snapshots, views, and concurrency support.

The primary goal of Plinth is to provide a storage abstraction optimized for analytical workloads while maintaining efficient ingestion and mutation support.

The core design principle:

> Published data is immutable. Changes are applied through new versions and background compaction.

This allows Plinth to provide:

- Concurrent readers without locking.
- Efficient columnar scans.
- Snapshot-based queries.
- Low-contention writes.
- Simple concurrency semantics.

---

# Architecture Overview

```
SQL Engine
    |
    v

Plinth
    |
    +----------------+
    | Dataframe      |
    +----------------+
            |
            v

    +----------------+
    | Chunk Manager  |
    +----------------+
            |
    +-------+-------+
    |               |
    v               v

Immutable       Mutable
Chunks          Chunk
(Read only)     (Write buffer)
```

---

# Core Concepts

## Dataframe

A Dataframe represents a logical table.

Responsibilities:

- Manage schema.
- Maintain chunk list.
- Provide snapshots.
- Coordinate writes.
- Trigger compaction.
- Manage persistence.

A dataframe does not directly store rows.

Example:

```
Dataframe: Users

Schema:

id      UInt64
name    String
age     UInt8


Chunks:

Chunk 0
Chunk 1
Chunk 2

Mutable Chunk
```

---

# Storage Layout

Plinth uses chunked columnar storage.

Hierarchy:

```
Dataframe
    |
    v
Chunks
    |
    v
Columns
    |
    v
Buffers
```

---

# Chunk

A Chunk is a batch of rows stored in columnar form.

A chunk becomes immutable after publication.

Example:

```
Chunk

Rows: 65536


Column: id

1
2
3


Column: name

Alice
Bob
Eve


Column: age

24
31
29
```

A chunk guarantees:

- All columns have the same row count.
- Data never changes after publication.
- Readers can access it without locks.

A chunk does not know about:

- SQL.
- Transactions.
- Queries.
- Other chunks.
- Indexes.

It only represents a block of columnar data.

---

# Column

A Column represents a typed sequence of values.

Examples:

```
Column<u64>

[1,2,3,4]


Column<String>

["Alice","Bob","Eve"]
```

A column is responsible for:

- Type information.
- Memory layout.
- Value access.
- Encoding.
- Compression.

Possible column types:

```
Integer
Float
Boolean
String
Date
Timestamp
Binary
```

---

# Buffer

A Buffer owns the physical memory behind a column.

Example:

```
Buffer<u64>

[1,2,3,4,5]
```

Responsibilities:

- Memory ownership.
- Allocation.
- Memory mapping.
- Serialization support.

---

# Write Path

## Mutable Chunk

New data is written into a mutable chunk.

Example:

```
Mutable Chunk

id      name

1       Alice
2       Bob
```

The mutable chunk grows until reaching a configured size.

Example:

```
Maximum chunk size:

65536 rows
```

---

# Freezing a Chunk

When a mutable chunk reaches its target size:

```
Mutable Chunk

        |
        |
        v

Immutable Chunk
```

The chunk becomes read-only.

A new mutable chunk is created.

Before:

```
Immutable:

Chunk 0
Chunk 1

Mutable:

Chunk 2
```

After:

```
Immutable:

Chunk 0
Chunk 1
Chunk 2

Mutable:

Chunk 3
```

---

# Read Path

Readers operate on immutable snapshots.

A query creates a snapshot:

```
Snapshot:

Chunk 0
Chunk 1
Chunk 2
```

The reader scans those chunks.

No locks are required.

Example:

```
Thread 1
    |
    v
Chunk 0


Thread 2
    |
    v
Chunk 1
```

Chunks can be processed independently.

---

# Concurrency Model

Plinth uses immutable snapshots.

## Readers

Readers:

- Acquire a snapshot.
- Hold references to chunks.
- Read without locks.

Example:

```rust
Arc<Chunk>
```

The reader never modifies the chunk.

---

## Writers

Writers:

- Append to mutable chunks.
- Freeze completed chunks.
- Publish immutable chunks.

Synchronization is only required for:

- Mutable chunk access.
- Publishing new chunks.
- Updating metadata.

The majority of query execution happens without locks.

---

# Snapshot Model

A snapshot represents a consistent view of a dataframe.

Example:

Version 1:

```
Chunk 0
Chunk 1
```

Writer creates:

```
Chunk 2
```

Version 2:

```
Chunk 0
Chunk 1
Chunk 2
```

Existing readers continue using Version 1.

New readers see Version 2.

---

# Deletes

Deletes use tombstones.

Rows are not immediately removed.

Example:

Before:

```
id      name

1       Alice
2       Bob
3       Eve
```

Delete Bob:

```
id      name      deleted

1       Alice     false
2       Bob       true
3       Eve       false
```

Queries ignore deleted rows.

A bitmap may be used instead of a boolean per row:

```
Validity bitmap:

101
```

---

# Updates

Updates are implemented as delete plus insert.

Example:

Before:

```
id=1
age=24
```

After:

```
Old row:

id=1
age=24
deleted=true


New row:

id=1
age=30
deleted=false
```

The old version is removed during compaction.

---

# Compaction

Tombstones eventually create wasted space.

A background compaction process rebuilds chunks.

Before:

```
Chunk 0

Alice
Bob   deleted
Eve
John  deleted
```

After:

```
Chunk 4

Alice
Eve
```

The new chunk replaces the old chunk.

---

# Compaction Trigger

Possible strategies:

```
deleted_rows / total_rows > threshold
```

Example:

```
deleted rows > 50%
```

or:

```
unused memory > threshold
```

Compaction runs asynchronously.

---

# Chunk Lifecycle

```
Create

  |
  v

Mutable Chunk

  |
  | freeze()

  v

Immutable Chunk

  |
  | deletes / updates

  v

Tombstones

  |
  | compaction

  v

New Immutable Chunk
```

---

# Persistence

Chunks are the persistence unit.

Example:

```
Table Metadata

      |

      v

Chunk Files

chunk_001
chunk_002
chunk_003
```

Advantages:

- Incremental persistence.
- Parallel loading.
- Simple recovery.
- Natural versioning.

---

# Views

Views reference existing chunks without copying data.

Example:

```
Dataframe:

Chunk 0
Chunk 1
Chunk 2


View:

references:

Chunk 1
Chunk 2
```

Views are lightweight.

---

# Proposed Rust Structure

```
plinth/

src/

    dataframe/
        dataframe.rs
        snapshot.rs

    chunk/
        chunk.rs
        builder.rs

    column/
        column.rs
        types.rs

    buffer/
        buffer.rs

    persistence/
        storage.rs

    compaction/
        planner.rs
        executor.rs

    schema/
        schema.rs
```

---

# Core Data Structures

Conceptual:

```rust
struct Dataframe {
    schema: Schema,

    chunks: Vec<Arc<Chunk>>,

    active_chunk: MutableChunk,
}


struct Chunk {
    columns: Vec<Column>,
    row_count: usize,
}


struct MutableChunk {
    builders: Vec<ColumnBuilder>,
}
```

---

# Design Goals

## Performance

- Sequential column scans.
- SIMD-friendly memory layout.
- Cache-efficient processing.
- Parallel chunk execution.

## Concurrency

- Lock-free reads.
- Minimal write locking.
- Snapshot isolation.

## Reliability

- Immutable published data.
- Safe persistence.
- Easy recovery.

## Extensibility

Future support:

- Compression.
- Dictionary encoding.
- Indexes.
- Vectorized execution.
- Distributed chunks.

---

# Summary

Plinth is a chunk-oriented, columnar storage engine.

The central design idea:

```
Mutable data is temporary.

Immutable data is permanent.

Changes create new versions.

Compaction removes old versions.
```

This allows Plinth to combine:

- Columnar analytics performance.
- Efficient ingestion.
- Concurrent reads.
- Snapshot semantics.
- Scalable storage management.
