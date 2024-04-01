# Storage

## Data structure

### Tags
* Each key gets mapped to an integer, its _key index value_.
* For each key, its values get turned into the _value index_. This is a map from string to int and used to "compress" the values. This map is ordered meaning the `i`-th value is lower than the `i+1`-th one, making binary operators fast.

### Tags on Objects
* Each object gets a bit-string for the keys used on that object. A `1` at bit position `i` means "The i-th key is set".
* A _key index_ is used to define the order/mapping of this bit-string, i.e. what key is behind which bit-index. So it's simply a map from key (in string form) to its index in the bit-string.
* The key index defined that _keys_ are set but not which _values_ are used. The _encoded values_ list stores the values of an object. It's a plain array and works as follows: The `j`-th element in the encoded value list corresponds to the `j`-th `1` in the key index and contains the number of the value from the _value index_. 
