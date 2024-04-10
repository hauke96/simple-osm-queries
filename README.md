# Simple OSM queries

Proof of concept of a tool to query OSM data, which is simple in its implementation as well as its usage.

## Query syntax

Queries consist of filter statements of the following form: `<location-specifier>.<object-type>{ <tag-filter> }`.
For example `bbox(1,2,3,4).nodes{ natural=tree }`.
Each statement is evaluated to a boolean value and there statements can be nested and used within the filters.

Top-level statements, i.e. statements that are not nested within some other statements, determine the output of the whole query.
Meaning: Any object fulfilling the filter criterion will be part of the output.

The keyword `this` refers to the object of the current statement (java programmers will recognize `this` ;) .
It enables you to call functions on this object as listed below.

### Functions on `this`

Usage: `this.<function-name>`.
No parentheses needed.

| Function name | Applies to                | Description                                                                                                                       |
|:--------------|:--------------------------|:----------------------------------------------------------------------------------------------------------------------------------|
| `nodes`       | Ways and relations        | The list of all nodes being part of the way or relation.                                                                          |
| `ways`        | Nodes and relations       | For nodes: The list of all ways the node is part of. For relations: All ways being part of this relation.                         |
| `relations`   | Nodes, ways and relations | For nodes and ways: The list of all relations this node/way is part of. For relations: All relations being part of this relation. |

### Examples

Benches near a way:

```go
// Get all nodes within this bbox, which ...
bbox(1, 2, 3, 4).nodes{
// ... are a bench or waste_basket AND ...
(amenity = bench OR amenity = waste_basket) AND
// ... have any highway-way within a 5m radius.
this.buffer(5m).ways{
highway = *
}
}
```

Same example but different way-filter:

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

Get all nodes with a house number that are part of a building outline:

```go
bbox(1, 2, 3, 4).nodes{
addr:housenumber = * AND
this.ways{
building = *
}
}
```

## Build and run

### Unit tests

Normal (without creating a coverage file) run `go test ./...`.

With coverage: Run `go test -coverprofile test.out ./...` and then `go tool cover -html=test.out` to view the coverage result.

Of course IDEs like Goland provide direct possibility to run the unit tests with and without coverage.

### CPU profiling

* Run with the `--diagnostics-profiling` flag to generate a `profiling.prof` file.
* Run `go tool pprof <executable> ./profiling.prof` so that the `pprof` console comes up.
* Enter `web` for a browser or `evince` for a PDF visualization