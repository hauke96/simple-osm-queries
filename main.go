package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/hauke96/sigolo/v2"
	"os"
	"runtime/pprof"
	"soq/importing"
	"soq/index"
	"soq/query"
	"strings"
)

const VERSION = "v0.1.0"

var cli struct {
	Logging              string      `help:"Logging verbosity." enum:"info,debug,trace" short:"l" default:"info"`
	Version              VersionFlag `help:"Print version information and quit" name:"version" short:"v"`
	DiagnosticsProfiling bool        `help:"Enable profiling and write results to ./profiling.prof."`
	Import               struct {
		Input string `help:"The input file. Either .osm or .osm.pbf." placeholder:"<input-file>" arg:"" type:"existingfile"`
	} `cmd:"" help:"Imports the given OSM file to use it in queries."`
	Query struct {
		Query string `help:"The query string." placeholder:"<query>" arg:""`
	} `cmd:"" help:"Returns the OSM data for the given query."`
}

var indexBaseFolder = "soq-index"
var defaultCellSize = 0.1

type VersionFlag string

func (v VersionFlag) Decode(ctx *kong.DecodeContext) error { return nil }
func (v VersionFlag) IsBool() bool                         { return true }
func (v VersionFlag) BeforeApply(app *kong.Kong, vars kong.Vars) error {
	fmt.Println(vars["version"])
	app.Exit(0)
	return nil
}

func main() {
	ctx := kong.Parse(
		&cli,
		kong.Name("Simple OSM queries"),
		kong.Description("A simple tool to query OSM data."),
		kong.Vars{
			"version": VERSION,
		},
	)

	if strings.ToLower(cli.Logging) == "debug" {
		sigolo.SetDefaultLogLevel(sigolo.LOG_DEBUG)
	} else if strings.ToLower(cli.Logging) == "trace" {
		sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	} else if strings.ToLower(cli.Logging) == "info" {
		sigolo.SetDefaultLogLevel(sigolo.LOG_INFO)
		sigolo.SetDefaultFormatFunctionAll(sigolo.LogPlain)
	} else {
		sigolo.SetDefaultFormatFunctionAll(sigolo.LogPlain)
		sigolo.Fatalf("Unknown logging level '%s'", cli.Logging)
	}

	if cli.DiagnosticsProfiling {
		f, err := os.Create("profiling.prof")
		sigolo.FatalCheck(err)

		err = pprof.StartCPUProfile(f)
		sigolo.FatalCheck(err)
		defer pprof.StopCPUProfile()
	}

	switch ctx.Command() {
	case "import <input>":
		err := importing.Import(cli.Import.Input, defaultCellSize, defaultCellSize, indexBaseFolder)
		sigolo.FatalCheck(err)
	case "query <query>":
		tagIndex, err := index.LoadTagIndex(indexBaseFolder)
		sigolo.FatalCheck(err)

		geometryIndex := index.LoadGridIndex(indexBaseFolder, defaultCellSize, defaultCellSize, tagIndex)

		q, err := query.ParseQueryString(`
// this is a comment
bbox(0.95,5.45,15,64.55).nodes{ amenity=bench}
`, tagIndex, geometryIndex)
		sigolo.FatalCheck(err)
		//query.ParseQueryString(`// this is a comment
		//
		//bbox(1,  2,3,
		//4).nodes{   amenity
		//=bench}`)

		features, err := q.Execute()
		sigolo.FatalCheck(err)

		sigolo.Debugf("Found %d features", len(features))

		err = index.WriteFeaturesAsGeoJson(features, tagIndex)
		sigolo.FatalCheck(err)
	default:
		sigolo.Errorf("Unknown command '%s'", ctx.Command())
	}
}
