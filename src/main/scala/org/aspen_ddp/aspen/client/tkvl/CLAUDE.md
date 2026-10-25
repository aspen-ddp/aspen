### Tiered Key Value List (TKVL)
- Very simple B-Tree like data structure implemented in terms of a hierarchy of linked lists.
- Trees use a KeyOrdering instance to sort keys. Current orderings include Integer, Lexical, and ByteArray
- Every time a new node is added to a list, an the minimum value of the new node and a pointer to the new object is
  inserted into the tier above it.
- Tier-0 holds the data, the upper tiers consist exclusively of (minimum_key, object-pointer) pairs
- The root of the tree is replaced every time the current root node is split into two nodes
- FinalizationActions are used to insert/remove entries in upper tiers as nodes are split/joined
- Left to right navigation in a tier is guaranteed to be consistent. Navigation between tiers is not
- A join absorbs a node's right neighbor, so a tier's rightmost node is never joined away. A join is also declined when
  the joined node would be larger than the neighbor, which happens when the emptied node's minimum is longer than the
  neighbor's. In both cases the node is left empty and `TailRemovalFinalizationAction` removes it: its predecessor
  (found by a descent through the upper tiers, not a scan of the tier) is pointed at the empty node's link, and its
  upper-tier entry is removed. If the neighbor's minimum is longer than the empty node's by then, the predecessor may
  no longer fit and splits in the normal manner; if it cannot be split, the node stays empty. Removing the upper-tier
  entries lets `JoinFinalizationAction` reduce the root by as many tiers as are left with a single entry.
- The size, split and join decisions of both encodings are in pure `plan*` functions (`KeyValueListNode` over a
  `NodeShape`, `DataObjectNodeEncoding` over a `NodeContent`); `insert`, `rename`, `delete`, `relink` and `splitAt`
  only apply the plan to a Transaction. The separation exists for testing: the node-size arithmetic is subtle and needs
  thousands of randomized cases, which are only practical without a running Aspen system (see `KeyValueNodePlanSuite`
  and `DataObjectNodePlanSuite`). Put new decision logic in the plan functions, not in the executors.
