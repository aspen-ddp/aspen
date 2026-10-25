### Tiered Key Value List (TKVL)
- Very simple B-Tree like data structure implemented in terms of a hierarchy of linked lists.
- Trees use a KeyOrdering instance to sort keys. Current orderings include Integer, Lexical, and ByteArray
- Every time a new node is added to a list, an the minimum value of the new node and a pointer to the new object is
  inserted into the tier above it.
- Tier-0 holds the data, the upper tiers consist exclusively of (minimum_key, object-pointer) pairs
- The root of the tree is replaced every time the current root node is split into two nodes
- FinalizationActions are used to insert/remove entries in upper tiers as nodes are split/joined
- Left to right navigation in a tier is guaranteed to be consistent. Navigation between tiers is not
- A join absorbs a node's right neighbor, so a tier's rightmost node is never joined away. When its last key is
  deleted, `TailRemovalFinalizationAction` unlinks it from its left neighbor (found by a descent through the upper tiers, not a scan of the tier) and removes its upper-tier entry, which
  lets `JoinFinalizationAction` reduce the root by as many tiers as are left with a single entry.
