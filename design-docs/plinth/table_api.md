# Table

`Table<T>` represents a logical relation whose rows are values of `T`.

The table is the owner of relational operations. Its responsibilities are deliberately limited to:

* inserting rows,
* deleting rows,
* acquiring snapshots.

The implementation of these operations belongs to the table itself. In particular, insertion is not delegated to `T`.

## Row type `T`

`T` defines the shape of a row and provides the information necessary for the table to access that row's fields.

A `T` implementation is responsible for describing its fields and providing access to the underlying data of those fields.

It is **not** responsible for performing table operations.

The distinction is:

> **`T` describes and exposes its data. `Table<T>` decides what to do with that data.**

The row implementation therefore must not know about:

* table storage,
* insertion,
* deletion,
* snapshots,
* Arrow builders,
* Arrow arrays,
* or any other storage-specific operation.

## Field representation

A field consists conceptually of two parts:

```text
Field metadata
├── name
└── type

Field value
└── data
```

The field's name and type describe the field, while its data is the value belonging to a particular row.

The type may use Arrow's type system where appropriate. This allows the structural description of `T` to map naturally onto the table's schema.

For example:

```text
User
├── id     → Int64   → <id>
├── name   → Utf8    → <name>
└── active → Boolean → <active>
```

The underlying data associated with a field must correspond to its declared type.

## Static metadata and row data

Field metadata is a property of the row type, not of an individual row.

For example, every `User` has the same structure:

```text
id     → Int64
name   → Utf8
active → Boolean
```

Only the values change between instances.

Therefore, the implementation must avoid materializing a collection of field descriptors for every row.

In particular, inserting a row should not require constructing a `Vec<Field>` or an equivalent heap-allocated representation.

Conceptually, the information is divided into:

```text
                    T
                    │
          ┌─────────┴─────────┐
          │                   │
       metadata             instance
          │                   │
    id → Int64              &id
    name → Utf8             &name
    active → Boolean        &active
```

The metadata belongs to the type, while access to the values is performed directly against the row instance.

Metadata must therefore be available independently of any particular row. This allows the table to construct a schema without first creating a row.

## Field access

`T` provides a mechanism for the table to access its fields without creating an intermediate collection.

Conceptually, this may take the form of a visitor or another statically known mechanism:

```rust
trait TableRow {
    fn visit_fields<V: FieldVisitor>(&self, visitor: &mut V);
}
```

The exact representation is an implementation detail.

The important property is that field access does not require allocating or constructing a runtime collection for each row.

A generated implementation can expose the fields directly from the row:

```text
row
│
├── field 0 → id
├── field 1 → name
└── field 2 → active
```

The table can then consume those values immediately.

The fields have a stable order, and that order is consistent with the type's metadata. This allows the table to deterministically associate each field with the corresponding column.

This allows the insertion path to operate directly on the underlying values:

```text
T
│
│ field access
▼
Table<T>::insert
│
│ direct write
▼
underlying storage
```

No intermediate row representation is required.

## Table-owned insertion

`Table<T>` is responsible for interpreting the fields provided by `T` and performing insertion.

Conceptually:

```rust
impl<T: TableRow> Table<T> {
    pub fn insert(&mut self, row: T) {
        // Access the fields of `row`.
        // Interpret them according to T's metadata.
        // Insert them into the table's underlying storage.
    }
}
```

The row implementation only provides access to the data.

It does not perform the insertion itself.

This is an important boundary. An implementation such as:

```rust
fn append_to(&self, columns: &mut Columns);
```

would be undesirable because it would make `T` aware of the table's storage model and would move insertion logic out of `Table<T>`.

Instead, the row implementation exposes structure and data, while the table owns the algorithm that consumes them.

The field access mechanism may expose values by reference or in another form appropriate for the insertion path. The design does not require a particular ownership strategy, as long as accessing the fields does not require constructing an intermediate collection.

## Schema

The same static field metadata used to describe `T` can be used to construct the relation's schema.

For example:

```text
T
│
└── field metadata
    ├── id     → Int64
    ├── name   → Utf8
    └── active → Boolean
```

can be transformed by the library into the corresponding Arrow schema fields.

This metadata is independent of any particular row instance.

Consequently, schema construction does not require creating field descriptors for each inserted row.

The relationship is:

```text
T's static metadata
        │
        ├── schema construction
        │
        └── interpretation during insertion
```

The row instance only supplies the changing data.

## Insertion without per-row allocation

The intended insertion path is therefore:

```text
             User
              │
              │ direct field access
              ▼
        Table<User>
              │
              │ interpret fields
              ▼
       Arrow-backed storage
```

For a row such as:

```rust
User {
    id: 42,
    name: "Alice".into(),
    active: true,
}
```

the table should be able to access the values directly rather than first constructing:

```text
[
    Field("id", Int64, 42),
    Field("name", Utf8, "Alice"),
    Field("active", Boolean, true),
]
```

The latter is an unnecessary intermediate representation.

The goal is that the abstraction itself does not introduce a heap allocation merely to describe the row. The underlying storage may of course allocate as necessary when growing buffers or storing data.

## Deletion

Deletion is owned by `Table<T>`.

The table defines the semantics of removing a row and performs the corresponding operation against its underlying storage.

`T` has no responsibility for deletion.

## Snapshots

A table can provide a snapshot of its current state.

The snapshot is acquired through `Table<T>` and represents the table's state according to the semantics defined by the table implementation.

The snapshot does not require `T` to know how the table is stored or how snapshots are implemented.

How a snapshot represents its data, how it shares storage with the table, and how it is physically backed are implementation details.

## Abstraction boundary

The design can be summarized as follows.

### `T`

`T` is responsible for:

* defining the logical row shape,
* describing its fields,
* providing field names and types,
* providing access to the data of each field,
* exposing that information without requiring a per-row field collection.

`T` is not responsible for:

* inserting data,
* deleting rows,
* manipulating table storage,
* constructing snapshots,
* or knowing how the table performs its operations.

### `Table<T>`

`Table<T>` is responsible for:

* representing the relation,
* enforcing the row type `T`,
* interpreting the field information provided by `T`,
* implementing insertion,
* implementing deletion,
* providing snapshots,
* and managing the underlying storage.

### Storage

The underlying storage is an implementation detail of the table.

The table may use Arrow arrays, builders, buffers, or another representation as required. Those details should not leak into the row abstraction.

The resulting dependency direction is:

```text
             T
             │
             │ structure + data access
             ▼
        ┌───────────┐
        │ Table<T>  │
        │           │
        │ insert    │
        │ delete    │
        │ snapshot  │
        └─────┬─────┘
              │
              │ owns / operates on
              ▼
       underlying storage
```

## Design principles

The design is based on a small number of principles:

> **`T` provides typed field information and direct access to its data.**

> **Field metadata belongs to the type, not to each row instance.**

> **`Table<T>` owns the interpretation of that data and all relational operations.**

> **The insertion path must not require allocating an intermediate collection merely to describe a row.**

> **Storage details must remain below the `Table<T>` abstraction.**

This keeps the row abstraction purely structural while giving `Table<T>` complete control over insertion, deletion, snapshots, schema construction, and the underlying storage strategy.
