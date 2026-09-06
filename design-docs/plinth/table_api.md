# Table

`Table<T>` represents a logical relation whose rows are values of `T`.

The table is the owner of relational operations. Its responsibilities are deliberately limited to:

* inserting rows,
* deleting rows,
* acquiring snapshots.

The table owns the implementation of these operations. In particular, insertion is implemented by `Table<T>` itself rather than being delegated to the row type.

## Row type `T`

`T` defines the shape of a row.

A type used as `T` must provide a way for the library to access its fields and the underlying data represented by those fields. This is the only responsibility of the row implementation.

The row type does **not** perform insertion and does not need to know how the table stores its data.

Conceptually:

```text
T
│
├── describes its fields
└── provides access to their underlying data
        │
        ▼
    Table<T>
        │
        ├── inserts rows
        ├── deletes rows
        └── acquires snapshots
```

The distinction is intentional:

> **`T` provides access to data. `Table<T>` decides what to do with that data.**

An implementation for `T` therefore must not contain operations such as `insert`, `append`, or `write`. Those operations belong exclusively to the table.

## Field access

The trait implemented by `T` provides structural access to its fields.

For example, a row type might conceptually expose:

```rust
trait TableRow {
    type Fields<'a>
    where
        Self: 'a;

    fn fields(&self) -> Self::Fields<'_>;
}
```

The exact representation of `Fields` is an implementation detail. It may be generated for the concrete row type, but its purpose remains the same: give the table access to the underlying values contained by `T`.

The implementation does not determine how those values are stored.

For an Arrow-backed table, for example, `T` does not construct Arrow arrays or append values to Arrow builders. The table receives the field data and performs the appropriate Arrow operations itself.

This keeps Arrow and other storage-specific concerns entirely within the table implementation.

## Schema

The same structural information provided by `T` can be used by the library to derive the relation's schema.

A row implementation describes the fields that make up `T`, including the information required to construct the corresponding `arrow::Field` values.

The transformation from the structural representation of `T` into the storage schema is a library concern.

Thus, the boundary remains:

```text
T
 └── describes fields and exposes their data

Table<T>
 ├── derives/owns the relation schema
 ├── inserts
 ├── deletes
 └── acquires snapshots
```

`T` describes **what a row is**. The table determines **how that row participates in the relation**.

## Insertion

Insertion is a responsibility of `Table<T>`.

When a value of `T` is supplied to `insert`, the table accesses its fields through the row interface and performs the insertion into its underlying storage.

Conceptually:

```rust
impl<T: TableRow> Table<T> {
    pub fn insert(&mut self, row: T) {
        let fields = row.fields();

        // Table owns the insertion algorithm.
        // It interprets `fields` and writes them to storage.
    }
}
```

The important property is that the algorithm is not split between the table and `T`.

The row implementation supplies the data; the table consumes it.

This means all row types use the same insertion mechanism, and changes to the storage or insertion strategy do not require changes to every row implementation.

## Deletion

Deletion is likewise owned by `Table<T>`.

The table defines what constitutes a row deletion and performs the corresponding operation against its underlying representation.

The row type has no knowledge of deletion.

This keeps deletion, like insertion, as a property of the relation rather than a property of an individual row value.

## Snapshots

A table can provide a snapshot of its current state.

The snapshot is a result of the table operation and is therefore acquired through `Table<T>`.

The internal structure of a snapshot is deliberately outside the responsibility of the `Table<T>` abstraction. The table only needs to provide the operation for obtaining one.

How a snapshot represents its data, how it shares storage, whether it is immutable, and how it is physically backed are separate implementation concerns.

## Abstraction boundary

The design can therefore be summarized as three responsibilities.

### `T`

`T` is responsible for:

* defining the logical row shape,
* exposing its fields,
* exposing the underlying data of those fields,
* providing the structural information required to describe those fields.

`T` is **not** responsible for:

* inserting itself,
* deleting itself,
* interacting with Arrow storage,
* knowing about table columns,
* constructing snapshots.

### `Table<T>`

`Table<T>` is responsible for:

* representing the relation,
* enforcing that inserted values conform to `T`,
* implementing insertion,
* implementing deletion,
* providing snapshots.

The table owns the algorithms associated with these operations.

### Storage

The underlying storage is an implementation detail of the table.

The table is free to use Arrow arrays, builders, buffers, or another representation as required. Those details should not leak into the row abstraction.

The resulting dependency direction is:

```text
             T
             │
             │ field/data access
             ▼
        ┌───────────┐
        │ Table<T>  │
        │           │
        │ insert    │
        │ delete    │
        │ snapshot  │
        └─────┬─────┘
              │
              │ owns/uses
              ▼
        underlying storage
```

The central design principle is therefore:

> **The row type exposes data; the table owns behavior.**

This keeps the row abstraction minimal while allowing `Table<T>` to retain complete control over insertion, deletion, snapshots, schema construction, and the underlying storage strategy.
