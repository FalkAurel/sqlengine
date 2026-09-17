# Row Deletion

Deleting rows is an integral part of any storage engine. Often times deletion happens **after** insertions happened, therefore we can not simply say when deleting, just remove the last element.

This raises an interesting question: if deletion is not structurally tied to insertion order, how do we know what element to delete? No matter how we answer that question, one thing is certain: We need some sort of addressing scheme.

## Finding a suitable addressing scheme

When implementing the addressing scheme, we need to consider what properties we'd like to observe with regards to row visibility and absolute positional ordering in the table.

### Case A:

Upon deletion all rows following the deleted row will be shifted by one index up such that `i + 1 ==> i`. This is the most intuitive and natural way to picture deletion, as it mimics the real world. For the purpose of this endeavour, which is building an SQL Engine on top of it, this is counterproductive. Implementing shifting dynamics would imply unstable indices and would render any indexing structure (B-Tree, HashMap, etc.) useless, as any deletion would have to be reflected in that structure as it occurs to maintain consistency.

### Case B:

Upon deletion all rows following the deleted row will not be shifted. This creates from the outside perspective a hole. It is not very intuitive, since it forces us to answer what the meaning, if any, of indexing into the deleted row is. On the other side it aligns very well with implementing anything of concurrent nature where immutability (and its implying stability) becomes invaluable. Furthermore, implementing it that way would not require us to update all other indices in our indexing structure.

--> Given the nature and large scope of the project, I believe choosing B is the right choice. The important distinction here is that we are no longer talking about an index in the traditional sense, but about an address into the table. Once we decide that positions are stable, an address can outlive the row that it points to. Therefore the question is no longer whether an address changes, but whether an address is still valid.

This distinction is important because it allows the storage engine to keep physical row addresses stable without requiring every consumer of those addresses to immediately know about deletion.


## Answering uncomfortable Questions about Case B

> **Q:** What is our strategy with indices pointing to deleted rows?

**A:** This problem only needs answering if and only if the index remains usable. In an ideal world, we'd connect row deletion to index deletion, which would reflect the semantic nature of this action.

> **Q:** How do we ensure that upon deletion the corresponding index is rendered unusable?

**A:** Ideally we'd enforce an eviction of that index from the managing datastructure. This decision is architecturally a very bad move, as it forces a very tight coupling between the storage engine (physikalische Ebene) and SQL Engine (konzeptionale Ebene). The other approach is to somehow make the index do nothing. The latter one is much more maintainable and will be used from this point onwards.

> **Q:** How do we know if an index is unusable, aka what part of the system manages that information?

**A:** For this question we have really only two ways: Either the table *remembers* what indexes are all invalid or we have the index tell us if it has become unusable.

The first approach is easy to implement, but comes with the problem that validity is now external to the address itself. In order to determine if an address is still valid, we have to perform another lookup into some managing datastructure. Even if that datastructure is hash-based, this only guarantees expected O(1) lookup behaviour. Hash collisions can require additional work and therefore make a strict O(1) guarantee impossible.

The latter approach is more challenging to implement, but allows the validity of an address to be determined as part of resolving the address itself. This gives us the O(1) lookup behaviour we want without introducing another source of truth for row validity.

The latter one is therefore the approach that will be used from this point onwards.

> **Summary:** Addressing will be implemented according to `Case B` and have its validity tied to itself.

