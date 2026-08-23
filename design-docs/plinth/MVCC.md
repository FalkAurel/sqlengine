# Overview of Multi-Version Concurrency Control

This design document is concerned with the **top layer of the storage stack: in-memory multi-version concurrency control (MVCC)**.

```text

┌──────────────────────────────────────────────┐

│              Top Layer / MVCC                │

│                                              │

│       In-Memory Multi-Version                │

│       Concurrency Control                    │

└──────────────────────┬───────────────────────┘

                   │

                   ▼

┌──────────────────────────────────────────────┐

│               Storage Engine                 │

└──────────────────────┬───────────────────────┘

                   │

                   ▼

┌──────────────────────────────────────────────┐

│                Apache Arrow                  │

│              (In-Memory Format)              │

└──────────────────────┬───────────────────────┘

                   │

                   ▼

┌──────────────────────────────────────────────┐

│               Apache Parquet                │

│                (File Format)                │

└──────────────────────┬───────────────────────┘

                   │

                   ▼

┌──────────────────────────────────────────────┐

│                    Disk                      │

└──────────────────────────────────────────────┘

```

The purpose of this layer is to ensure that **reads remain consistent and repeatable for a given snapshot**. A reader should observe a stable view of the data regardless of concurrent writes taking place elsewhere.

The design is based on three properties:

1. Published data is immutable.

2. Writes become visible atomically at commit time.

3. Readers acquire a snapshot of the committed history and never change that snapshot during their lifetime.

This allows readers and writers to operate concurrently without requiring readers to lock the underlying data.

## What Is a Read Access?

A **read access** is an operation that does not mutate the underlying data. Because it is non-mutating, multiple readers can access the same snapshot concurrently without interfering with one another.

For example, reading a record, scanning a set of records, or querying an existing version of the data constitutes a read access.

From now on, a read access will also be referred to as a **snapshot**.

A snapshot has one important property:

> Once a snapshot has been created, its view of the database must not change during its lifetime.

If a snapshot starts at version `n`, concurrent commits may create versions `n + 1`, `n + 2`, etc., but the existing snapshot continues to observe version `n`.

## What Is a Write Access?

A **write access** is an operation that modifies the logical state of the data.

This includes:

* Inserting a record

* Updating a record

* Deleting a record

Because the storage architecture is append-only, updates are modeled as a combination of a `delete` and an `insert`.

The physical data produced by a write is not immediately visible to readers. Instead, a write first prepares its changes and only makes them visible when it commits.

This gives us the following conceptual lifecycle:

```text

prepare

│

▼

unpublished changes

│

▼

commit

│

▼

globally visible

```

## Design Choices in Our Favour

When thinking about column stores, DuckDB provides a useful mental model. Full memory segments are never mutated, allowing readers to access them without synchronizing with writers.

We want to preserve this property.

Our architecture is therefore fundamentally **append-only**:

* Existing published data is never mutated.

* Inserts append new data.

* Updates are modeled as `delete + insert`.

* Deletes are represented through metadata rather than by modifying the underlying data.

This significantly restricts the synchronization problem.

Readers do not need to synchronize with writers when accessing immutable data. The main problem becomes determining **when a deletion becomes visible** and **which committed state a snapshot should observe**.

# MVCC Model

The MVCC layer consists of four conceptual components:

```text

Transaction

│

├── prepares changes

│

▼

Commit

│

├── assigns commit version

│

▼

Stable Version

│

▼

Snapshot

│

▼

Visibility

```

The important distinction is between a **transaction** and a **version**.

A transaction identifies a logical set of changes.

A version identifies a position in the globally committed history.

A transaction therefore does not receive its final version when it starts. Its version is assigned when it commits.

This distinction is important for prepared changes. A transaction may need to prepare deletes, inserts, and updates before its commit version is known. Prepared changes therefore refer to the transaction itself, rather than to a future version.

For example:

Transaction T₁

    delete X
    insert Y

During preparation, the changes are conceptually:

Delete(X, T₁)
Insert(Y, T₁)

No commit version is required yet.

At the commit point, the transaction receives its version:

T₁.commit_version = v₄

The prepared changes then become part of version v₄ together, when the transaction is published.

There are therefore two distinct version concepts:

snapshot_version
    │
    └── version of committed history observed by the transaction

commit_version
    │
    └── version assigned to the transaction when it commits

The snapshot version is known when the transaction creates its snapshot. The commit version does not exist until the transaction reaches the commit point.

The commit version is therefore a property of the committed transaction, rather than something that must be known while preparing each individual operation.

## Terminology

### Transaction

A **transaction** is a logical unit of modification.

It may contain multiple operations:

```text

Transaction T₁

delete X

delete Y

insert Z

update A

```

All operations belonging to the transaction become visible together when the transaction commits.

### Commit

A **commit** is the final publication step of a transaction.

A commit guarantees that all changes belonging to the transaction are complete before they become visible to subsequently created snapshots.

### Version

A **version** represents a position in the globally committed history.

Versions are assigned **at commit time**, not when a transaction starts and not when a write begins preparing its changes.

For example:

```text

T₁: prepare ───────────────────► commit → v₁

T₂: prepare ───────► commit → v₂

```

The physical execution order of transactions does not determine their version. The order in which transactions are committed determines their position in the committed history.

### Stable Version

The **stable version** is the highest version that has been committed and published to readers.

If the stable version is `v₂`, then all changes belonging to commits through `v₂` are complete and globally visible.

The central invariant is:

> **If a snapshot observes stable version `vₙ`, it observes the complete state produced by all commits up to and including `vₙ`.**

Once a snapshot has acquired `vₙ`, its version never changes.

```text

Stable version:

v₀ ──► v₁ ──► v₂ ──► v₃

             │

             │

             ▼

        Snapshot S

Snapshot S remains at v₂ even after

the stable version advances to v₃.

```

# Problem 1: Synchronisation Between Reads and Writes

The append-only architecture means that we do not need to synchronize readers with writers when reading immutable data.

The main question is instead:

> **At what point does a write become visible to a snapshot?**

Consider two concurrent writes:

```text

Reading access:   |------ R₁ ------|

Writing access:   |----------- W₁ ---------|

Writing access:         |------ W₂ ------|

Reading access:                              |---- R₂ ----|

```

`R₁` starts before either write commits, so it must observe the state before both writes.

`R₂` starts after both writes have committed, so it must observe the state after both writes.

The difficult case occurs when physical execution order and logical write order differ.

## Physical Execution Order Is Not Version Order

Consider:

```text

Reading access: |------- R₁ -------|

Writing access:   |----------------- W₁ -----------------|

Writing access:      |---- W₂ ----|

Reading access:                    |----- R₂ -----|

Reading access:                                            |--- R₃ ---|

```

`W₂` may finish its preparation before `W₁`, but this does not mean that `W₂` should immediately become version `v₂`.

Instead, versions are assigned at the commit point.

For example:

```text

W₁: prepare ───────────────────► commit → v₁

W₂: prepare ───────► commit → v₂

```

or, if `W₂` commits first:

```text

W₂: prepare ───────► commit → v₁

W₁: prepare ───────────────────► commit → v₂

```

The physical execution order is irrelevant to snapshot consistency.

What matters is the order in which changes become part of the globally committed history.

This gives us the following invariant:

> **A version must never be published before all changes belonging to that version have completed.**

Consequently, the version counter should not describe an in-progress write. It should describe the **committed history**.

## Snapshot Creation

A snapshot therefore only needs to acquire the current stable version:

```text

snapshot.version = stable_version

```

Conceptually:

```text

                stable_version

                      │

                      ▼

               ┌────────────┐

               │     v₄     │

               └─────┬──────┘

                     │

          ┌──────────┴──────────┐

          ▼                     ▼

     Snapshot A            Snapshot B

        v₄                    v₄

```

Both snapshots observe the same committed state.

If a writer subsequently commits `v₅`:

```text

                stable_version

                      │

                      ▼

                     v₅

                      │

          ┌───────────┴───────────┐

          ▼                       ▼

     Snapshot A/B            Snapshot C

        v₄                        v₅

```

Snapshot A remains at `v₄`, while Snapshot C observes `v₅`.

## Publishing a Commit

The commit operation therefore has two responsibilities:

1. Ensure that all changes belonging to the transaction are complete.

2. Publish the new version to readers.

Conceptually:

```text

Transaction

 │

 ▼

Prepare changes

 │

 ▼

Acquire commit point

 │

 ├── assign next version

 │

 ├── publish transaction changes

 │

 └── advance stable version

 │

 ▼

Release commit point

```

The commit point is the only place where writers need to coordinate with one another.

Readers do not need to acquire this lock.

This gives us the desired asymmetry:

```text

Readers:

    atomic snapshot acquisition

    no writer lock

    no waiting for writers

Writers:

    concurrent preparation

    serialized commit/publication

```

# Memory Ordering

Atomicity and memory ordering are separate concerns.

Atomicity answers:

> Can multiple threads safely access the version counter?

Memory ordering answers:

> If a reader observes a particular version, what other state is it guaranteed to observe?

The second question is important for MVCC.

A writer must not publish version `vₙ` before the state belonging to `vₙ` is ready.

Conceptually:

```text

Writer                              Reader

prepare changes

  │

  ▼

changes complete

  │

  │ release

  ▼

stable_version = n

                               │

                               │ acquire

                               ▼

                          observe vₙ

                               │

                               ▼

                          read vₙ

```

Therefore, we are not merely interested in the atomicity of the version counter. We also need an ordering guarantee between publication of the version and observation of the state associated with that version.

For this reason, the version publication mechanism requires at least **acquire/release semantics**.

We do not necessarily need a total sequential ordering across every atomic operation in the system. The important ordering is the relationship between:

* the writer preparing and publishing a committed state, and

* the reader acquiring the corresponding snapshot.

# Problem 2: Managing Concurrent Writes

Writes may execute concurrently during their preparation phase.

The synchronization requirement is concentrated at the commit point.

Consider:

```text

T₁: prepare ────────────────────┐

                                │

T₂: prepare ────────────┐       │

                        │       │

                        ▼       ▼

                     commit point

                          │

                          ▼

                     stable\_version++

```

Only one transaction may publish a new committed version at a time.

This provides a total ordering over **committed transactions**, without requiring a total ordering over their physical execution.

The resulting model is:

```text

Physical execution:

T₁ ────────────────────────────┐

                               │

T₂ ────────────────────┐       │

                       │       │

                       ▼       ▼

Committed history:

                       T₂ → v₁ → T₁ → v₂

```

The physical execution order and committed history can differ.

That is acceptable because the committed history is what snapshots observe.

**# Prepared Changes and Commit Version Assignment

The fact that the commit version is unknown during preparation is intentional.

A transaction prepares its logical changes without assigning them versions. The transaction identity acts as the temporary owner of those changes.

For example:
```text
Transaction T₁

    delete X
    delete Y
    insert Z

During preparation:

T₁:
    Delete(X)
    Delete(Y)
    Insert(Z)

commit_version = unset
```

At the commit point:

```text
Acquire commit point
        │
        ▼
Assign next version
        │
        ▼
T₁.commit_version = v₄
        │
        ▼
Publish all T₁ changes
        │
        ▼
Advance stable_version to v₄

```

The important invariant is:

> A transaction does not need a commit version while it is being prepared. Its commit version is assigned exactly once at the commit point, before the transaction becomes visible.

Before commit, a deletion can therefore be represented as transaction-owned state:

Delete(X, transaction=T₁)

After commit:

T₁.commit_version = v₄

and the deletion is logically associated with v₄.

The underlying published record remains immutable. The deletion is represented through separate metadata or transaction-owned visibility information rather than by mutating the original record.

Commit Version as the Visibility Boundary

All changes belonging to a transaction receive the same commit version.

For example:

Transaction T₁

    delete X
    delete Y
    insert Z

commit → v₄

The resulting visibility is:

Snapshot v₃:

    X → visible
    Y → visible
    Z → invisible

Snapshot v₄:

    X → deleted
    Y → deleted
    Z → visible

There is no committed state in which only part of T₁ is visible.

The commit version therefore acts as the atomic visibility boundary for the entire transaction.

A useful visibility rule for a versioned record is:

visible(record, snapshot_version) iff

    create_version <= snapshot_version
    AND
    (
        delete_version is unset
        OR
        snapshot_version < delete_version
    )

For a prepared deletion, delete_version is not known during preparation. It becomes the transaction's commit_version when the transaction commits.

For an update represented as delete + insert:

T₁:

    delete X@old
    insert X@new

commit → v₅

the old version becomes invisible at v₅, while the new version becomes visible at v₅.

Therefore:

snapshot < v₅  → X@old
snapshot >= v₅ → X@new

The transaction's commit version provides the atomic boundary for both operations.

Deletion**

Deletion is particularly well suited to the append-only architecture.

A delete does not physically remove or modify the existing data. Instead, it records that an entry is no longer visible from a particular committed version.

For example:

```text

Record X

created:  v₀

deleted:  v₃

```

The record is therefore visible to snapshots before `v₃` and invisible to snapshots at or after `v₃`.

Conceptually:

```text

      v₀       v₁       v₂       v₃       v₄

       │        │        │        │        │

X      ███████████████████████████

                                  │

                                  ▼

                               deleted

```

This allows readers to determine visibility without modifying the underlying record.

## Deletion and Transactions

Deletion becomes particularly simple when it is associated with a transaction.

Consider:

```text

Transaction T₁

delete X

delete Y

commit → v₄

```

Both deletions become visible together:

```text

Snapshot v₃:

X → visible

Y → visible

Snapshot v₄:

X → deleted

Y → deleted

```

There is no valid committed state in which `X` is deleted while `Y` is still visible.

The transaction therefore provides the atomicity boundary.

This is more important than deletion itself being idempotent.

## Idempotent Deletion

Deleting the same record twice is naturally idempotent:

```text

delete(X)

delete(X)

≡

delete(X)

```

This means that repeated attempts to delete the same record do not change its final logical state.

However, idempotency does **not** provide atomicity across different records.

For example:

```text

Transaction T₁:

delete X

delete Y

```

We must not allow:

```text

Snapshot v₄:

X → deleted

Y → visible

```

if both deletions belong to the same transaction.

Instead, both changes must become visible at the transaction's commit version:

```text

                commit T₁

                    │

                    ▼

                   v₄

             ┌──────┴──────┐

             ▼             ▼

         delete X       delete Y

```

Therefore:

> **Idempotency protects the logical state of an individual deletion. Transactional commit provides atomic visibility across multiple deletions.**

# Updates

Because published data is immutable, an update is represented as:

```text

update(X, new_value)

    │

    ├── delete old X

    │

    └── insert new X

```

Both operations belong to the same transaction.

For example:

```text

Transaction T₁:

delete X\@old

insert X\@new

commit → v₅

```

A snapshot before `v₅` observes:

```text

X = old

```

A snapshot at or after `v₅` observes:

```text

X = new

```

There must never be a committed snapshot in which the old version has disappeared without the new version being visible.

Again, the commit boundary provides the atomicity.

# Inserts

Inserts are the simplest operation in this model.

The new data can be prepared independently of readers:

```text

prepare:

append new data

construct metadata

```

The data remains unpublished until commit.

At commit:

```text

commit → v₆

```

the new data becomes visible to snapshots created at `v₆` or later.

Existing snapshots remain unaffected because they continue to operate on earlier versions.

# Commit Protocol

The resulting write protocol can be summarized as:

```text

                ┌───────────────┐

                │  Transaction  │

                └───────┬───────┘

                        │

                        ▼

                   Prepare data

                        │

                        ▼

                Prepare metadata

                        │

                        ▼

                Acquire commit lock

                        │

                        ▼

              Assign next commit version

                        │

                        ▼

                Publish transaction

                        │

                        ▼

              Advance stable\_version

                        │

                        ▼

               Release commit lock

```

The important point is that the expensive work does not need to happen while holding the commit lock.

Only the publication step needs to be serialized.

This allows transactions to prepare concurrently:

```text

T₁ ────────────────────────────┐

T₂ ────────────────────┐       │

T₃ ────────────┐       │       │

               │       │       │

               ▼       ▼       ▼

             prepare concurrently

                       │

                       ▼

                serialize commit

```

# Read Protocol

The read path is intentionally much simpler.

A reader performs:

```text

1. Acquire stable_version

2. Store it as snapshot.version

3. Read immutable data

4. Apply visibility rules using snapshot.version

```

Conceptually:

```text

Reader

acquire(stable\_version)

          │

          ▼

    snapshot = vₙ

          │

          ▼

   read immutable data

          │

          ▼

  evaluate visibility

          │

          ▼

      result

```

No reader lock is required.

A writer committing after step 1 does not affect the snapshot.

# Concurrency Properties

This design provides the following properties.

### Readers do not block writers

Readers only need to acquire the current stable version. They do not need to wait for an in-progress transaction to finish.

### Writers do not mutate published data

Once data is visible to a snapshot, it remains immutable.

### Writers may prepare concurrently

Multiple transactions can construct their changes independently.

### Commits are ordered

The commit point establishes a total ordering over globally visible transactions.

### Snapshots are stable

A snapshot's version never changes during its lifetime.

### Transactional changes become visible atomically

All changes belonging to a transaction become visible at the transaction's commit version.

# Summary

The core idea of this MVCC implementation is therefore:

> **Prepare concurrently, commit serially, read immutably.**

The architecture can be summarized as:

```text

                WRITE PATH

         ┌──────────────────┐

         │    Transaction   │

         └────────┬─────────┘

                  │

                  ▼

            Prepare changes

                  │

                  ▼

          Acquire commit lock

                  │

                  ▼

          Assign commit version

                  │

                  ▼

           Publish changes

                  │

                  ▼

          Advance stable version

                  │

                  ▼

         Release commit lock



                READ PATH

         ┌──────────────────┐

         │      Reader      │

         └────────┬─────────┘

                  │

                  ▼

         Acquire stable version

                  │

                  ▼

             Create snapshot

                  │

                  ▼

         Read immutable data

                  │

                  ▼

         Apply visibility rules

```

The key invariant tying the entire design together is:

> **A committed version represents a complete, immutable point in the globally visible history.**

Everything before that version is part of the snapshot. Everything after that version is invisible to it.

This allows the synchronization problem to be reduced to a small commit protocol while keeping the read path extremely cheap.