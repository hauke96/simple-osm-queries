# Simple OSM queries

Proof of concept of a tool to query OSM data, which is simple in its implementation as well as its usage.

## Usage

### Import

Usage: `go run . import your-file.osm.pbf`

Performance comparison (as of 2024-04-13; SSD + 10 year old Intel Xeon E3-1231 v3):
* The index structure is 2.5 to 3 times as large as the raw `.osm.pbf` file.
* The import takes longer the more data there is (s. numbers below) but on my machine runs with 2.5 to 4.5 MB/s.
* Examples
  * The `hamburg-latest.osm.pbf` (~46 MB) takes ~10 s, the cache will be ~125 MB large.
  * The `germany-latest.osm.pbf` (~4.1 GB) takes ~25 min., the cache will be 11.1 GB large.

### Query

TODO

Performance comparison:
* The query `bbox(1.640,45.489,19.198,57.807).nodes{ amenity=bench AND seats=* }` (whole Germany using `germany-latext.osm.pbf`) takes ~1:35 min. (SSD + 10 year old Intel Xeon E3-1231 v3), which is about the same using Overpass.

### Server

Usage: `go run . server`

This starts an HTTP server on Port 8080. Use [localhost:8080/app](http://localhost:8080/app) to access a simple web-interface.
HTTP POST requests with the query as body go to [localhost:8080/query](http://localhost:8080/query) and return GeoJSON.

## Query language

Queries consist of *statements*, *object types* and *expressions*.

A statement is the bigger picture and consists of several elements:
* A *location expression* defining *where* to search. 
* An *object type* defining what *type* to consider.
* A list of *filter expressions* defining *what* to search. This list might contain sub-statements (s. below).

A statement has the following form: `<location-expression>.<object-type>{ <filter-expression> }`.
For example `bbox(1,2,3,4).nodes{ natural=tree }`.

### Output

Only top-level statements, i.e. statements that are not nested within some other statements (s. below), determine the output of the whole query.
Meaning: Any object fulfilling the filter criterion will be part of the output.

### Sub-statements

Now the tricky part:
Statements can be nested using the `this` specifier, for example: `this.ways{ highway=primary}`.
These statements are called nested statement, inner statement, sub-statement (as used here frequently) or (more technically correct) *context-aware statement*.

Here a bigger example on how this works:
```go
bbox(1, 2, 3, 4).nodes{
    addr:housenumber = * AND
    this.ways{
        building=*
    }
}
```
This query outputs all nodes, which have a house number *and* are part of a building-way (which is often the case for entrance-nodes having a house number).
The `this.ways{...}` statement considers the ways *this* node is part of (therefore the term "context-aware" because the result depends on the current considered node).

### Functions on `this`

Usage: `this.<function-name>`.
No parentheses needed.

| Function name | Applies to                | Description                                                                                                                       |
|:--------------|:--------------------------|:----------------------------------------------------------------------------------------------------------------------------------|
| `nodes`       | Ways and relations        | The list of all nodes being part of the way or relation.                                                                          |
| `ways`        | Nodes and relations       | For nodes: The list of all ways the node is part of. For relations: All ways being part of this relation.                         |
| `relations`   | Nodes, ways and relations | For nodes and ways: The list of all relations this node/way is part of. For relations: All relations being part of this relation. |

![](this-node-way-relations.png)

### Examples

Find all benches with missing `seats` tag:
```go
// Get all nodes within this bbox, which ...
bbox(1, 2, 3, 4).nodes{
    // ... are a bench without given number of seats
    amenity = bench AND seats!=*
}
```

Find all ways with an address, that have a node on them (e.g. an entrance) that also has an address:
```go
bbox(1, 2, 3, 4).ways{
    addr:housenumber=* AND
    this.nodes{
        addr:housenumber=*
    }
}
```

### Future features (not yet implemented)

Find alls benches near a street/path:
```go
// Get all nodes within this bbox, which ...
bbox(1, 2, 3, 4).nodes{
    // ... are a bench or waste_basket AND ...
    (amenity = bench OR amenity = waste_basket) AND
    // ... have any highway-way within a 5m radius.
    this.buffer(5m).ways{
        highway=*
    }
}
```

Same example but different way-filter using a buffer around the node:

```go
// Get all nodes within this bbox, which ...
bbox(1, 2, 3, 4).nodes{
    // ... are a bench or waste_basket AND ...
    (amenity = bench OR amenity = waste_basket) AND
    // ... all ways in a 5m radius are highway-ways.
    !this.buffer(5m).ways{
        highway!=*
    }
}
```

## Development

See the [README.md](src/README.md) in the `src` folder for further details.