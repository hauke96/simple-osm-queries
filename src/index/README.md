# Indices

There are several index structures that are used to speed up queries.

## Tag index

This is not an index as you might know from database columns, but a way to efficiently encode tags on objects.
This eliminates the need for string comparisons and maybe even parsing of numeric strings.
Instead, only comparisons of numbers are needed.

### Data structure

#### Tags
* Each key gets mapped to an integer, its _key index value_.
* For each key, its values get turned into the _value index_. This is a map from string to int and used to "compress" the values. This map is ordered meaning the `i`-th value is lower than the `i+1`-th one, making binary operators fast.

#### Tags on Objects
* Each object gets a bit-string for the keys used on that object. A `1` at bit position `i` means "The i-th key is set".
* A _key index_ is used to define the order/mapping of this bit-string, i.e. what key is behind which bit-index. So it's simply a map from key (in string form) to its index in the bit-string.
* The key index defined that _keys_ are set but not which _values_ are used. The _encoded values_ list stores the values of an object. It's a plain array and works as follows: The `j`-th element in the encoded value list corresponds to the `j`-th `1` in the key index and contains the number of the value from the _value index_. 

## Geometry index

This index structure places a grid over the world and stores each cell into a separate file.
Other indices, like an R-tree, might be implemented in the future.

The data structure _must_ be able to be persisted and _must_ be able to stay _out_ of memory.
This is important for huge, maybe even world wide, use cases, where a complete in-memory solution would not be feasible regarding the memory consumption.
For this memory reason, it's also important that the structure allows paged/batch/grouped processing, i.e. processing certain data at once and then loading the next bunch of data.

There are some assumptions that lead to the decision for this structure:
1. Most queries are probably not spatially huge. Is is assumed that the majority of queries is within the area of a mid-sized city (like 20x20km or so).
2. Most queries are done using a BBOX, so no polygonal shape. Therefore, complex index structures _might_ not be overly beneficial compared to this simple grid approach.
