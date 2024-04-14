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
        highway=*
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
        building=*
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