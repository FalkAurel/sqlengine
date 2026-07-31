# Plinth Storage Design

## Overview

Plinth uses a **segmented columnar storage model** designed for high-performance in-memory analytical workloads.

The goal of the storage layer is to provide:

- Efficient sequential scans
- Cache-friendly memory access
- Cheap data growth
- Stable memory locations
- Simple ownership semantics
- Parallel execution capabilities

The storage hierarchy is:

```
Dataframe
    |
    +-- Column
            |
            +-- Segment
                    |
                    +-- Block
                            |
                            +-- Element
```

Each layer has a clearly defined responsibility:

| Component | Responsibility |
|---|---|
| Dataframe | Logical collection of columns |
| Column | Stores values of one data type |
| Segment | Stable memory allocation unit |
| Block | Execution and access unit |
| Element | Individual value |

---

# Design Motivation

A naive contiguous storage design creates excellent scan performance:

```
Column

+--------------------------------+
| Block | Block | Block | Block |
+--------------------------------+
```

However, growing the table becomes expensive.

If the allocation fills up:

```
Before:

+----------------+
| B0 | B1 | B2 |
+----------------+

After:

+------------------------+
| B0 | B1 | B2 | B3 |
+------------------------+
```

The underlying memory region may need to be relocated:

1. Allocate a larger memory region.
2. Copy all existing data.
3. Release the old allocation.

For large in-memory tables this can result in unnecessary memory movement.

Plinth therefore uses **segmented allocation**.

---

# Segmented Storage Model

A column consists of multiple independently allocated segments.

```
Column<T>

+--------------------------------+
| Segment 0                      |
+--------------------------------+
| Segment 1                      |
+--------------------------------+
| Segment 2                      |
+--------------------------------+
```

Each segment contains a contiguous collection of blocks.

```
Segment

+------------------------------------------------+
| Block 0 | Block 1 | Block 2 | ... | Block N    |
+------------------------------------------------+
```

Segments are allocated independently and never moved after creation.

This provides:

- Stable memory addresses
- Append-only growth
- No large reallocations
- Efficient sequential scans

---

# Column

A `Column` represents a single typed column in a dataframe.

A column owns its segments.

Conceptually:

```rust
struct Column<T> {
    segments: Vec<Segment<T>>,
    length: usize,
}
```

Properties:

- All values inside a column share the same type.
- Elements are stored in blocks.
- Segments are ordered by insertion time.
- Existing segments are immutable after creation except for appending.

---

# Segment

A `Segment` is the primary allocation unit.

A segment groups multiple blocks together into one stable memory region.

Conceptually:

```rust
struct Segment<T> {
    blocks: Box<[Block<T>]>,
}
```

Memory layout:

```
Segment

+------------------------------------------------+
| Block 0 | Block 1 | Block 2 | ... | Block 127  |
+------------------------------------------------+
```

A segment provides:

- Memory stability
- Efficient block iteration
- Parallel execution boundaries
- Reduced allocation overhead

---

# Block

A `Block` is the smallest execution unit in Plinth.

A block contains a fixed-capacity array of elements of one type.

Conceptually:

```rust
struct Block<T> {
    length: u16,
    values: [T; 1024],
}
```

Memory layout:

```
Block<T>

+------------------------------------------------+
| Metadata                                       |
+------------------------------------------------+
| Element 0                                      |
| Element 1                                      |
| Element 2                                      |
| ...                                            |
| Element 1023                                   |
+------------------------------------------------+
```

---

# Block Invariants

Every block guarantees:

## Homogeneous Data

All elements inside a block have the same type.

Example:

```
Block<i64>

[i64, i64, i64, i64, ...]
```

Mixed types are not allowed.

---

## Contiguous Memory

Elements are stored sequentially.

```
+---------+---------+---------+-----+-----------+
| Elem 0  | Elem 1  | Elem 2  | ... | Elem 1023 |
+---------+---------+---------+-----+-----------+
```

No element-level pointers exist.

---

## Natural Alignment

The first element of a block starts at the natural alignment boundary of its type.

Example:

```
Type: u64
Alignment: 8 bytes

0x1000 Element 0
0x1008 Element 1
0x1010 Element 2
```

Because elements have equal size and alignment, all elements remain naturally aligned.

---

## Fixed Capacity

A block has capacity for:

```
1024 Elements
```

The actual number of valid elements may be smaller.

Example:

```
Block

Capacity: 1024

Valid:
[Element 0 ... Element 823]

Unused:
[Element 824 ... Element 1023]
```

This allows the final block of a column to be partially filled.

---

# Element Addressing

Elements are accessed using arithmetic rather than traversal.

For a block capacity of 1024:

```
block_index = row_id / 1024

offset = row_id % 1024
```

Example:

```
row_id = 5000

block_index:

5000 / 1024 = 4


offset:

5000 % 1024 = 904
```

The element location becomes:

```
Segment containing Block 4

Block 4

Element 904
```

No linked-list traversal is required.

---

# Growth Strategy

Data is appended by filling the current segment.

When the segment reaches capacity, a new segment is allocated.

Example:

Before:

```
Column

Segment 0

+-----------------------------+
| B0 | B1 | B2 | B3            |
+-----------------------------+
```

After growth:

```
Column

Segment 0

+-----------------------------+
| B0 | B1 | B2 | B3            |
+-----------------------------+

Segment 1

+-----------------------------+
| B4 | B5 | B6 | B7            |
+-----------------------------+
```

Existing memory remains untouched.

---

# Access Pattern

Scanning a column:

```
for segment in column:

    for block in segment:

        process(block)
```

The CPU observes mostly sequential access:

```
Segment 0:

[B0][B1][B2][B3]


Segment 1:

[B4][B5][B6][B7]
```

Benefits:

- Hardware prefetching
- Cache efficiency
- SIMD execution
- Parallel processing

---

# Concurrency Model

Synchronization happens at the block or segment level.

Individual elements are not synchronized.

A block has a limited access state:

```
+-------------+
| Available   |
+-------------+
      |
      |
+-------------+
| Reading     |
+-------------+

or

+-------------+
| Writing     |
+-------------+
```

Rules:

- Multiple readers may access a block concurrently.
- Writers require exclusive access.
- Reading and writing cannot happen simultaneously.

---

# Parallel Execution

Segments provide natural execution boundaries.

Example:

```
Column

Segment 0
    |
    +-- Worker 1


Segment 1
    |
    +-- Worker 2


Segment 2
    |
    +-- Worker 3
```

Workers can process independent segments without interfering.

---

# Memory Characteristics

For a column of type `T`:

```
Block Size:

1024 * sizeof(T)
```

Examples:

## u8

```
1024 * 1 byte

= 1024 bytes
```

## u64

```
1024 * 8 bytes

= 8192 bytes
```

The block size is intentionally small enough for efficient execution while large enough to amortize overhead.

---

# Final Architecture

The final storage hierarchy:

```
Dataframe

    Column<T>

        Segment

            Block<T>

                Element
```

The design avoids:

- Linked lists of blocks
- Large memory reallocations
- Element-level indirection
- Unstable memory addresses

while providing:

- Fast scans
- Efficient growth
- Stable allocations
- Simple ownership
- Parallel execution support

Plinth therefore behaves as a **segmented in-memory column store**, optimized for SQL execution workloads.
