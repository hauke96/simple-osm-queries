Source code of the project.

## Build and run

Simply use `go build .` and `go run .`.

## Testing

### Unit tests

Normal (without creating a coverage file) run `go test ./...`.

With coverage: Run `go test -coverprofile test.out ./...` and then `go tool cover -html=test.out` to view the coverage result.

Of course IDEs like Goland provide direct possibility to run the unit tests with and without coverage.

### CPU profiling

* Run with the `--diagnostics-profiling` flag to generate a `profiling.prof` file.
* Run `go tool pprof <executable> ./profiling.prof` so that the `pprof` console comes up.
* Enter `web` for a browser or `evince` for a PDF visualization