package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/hauke96/sigolo/v2"
	"soq/importing"
	"soq/index"
	"soq/query"
	"strings"
)

const VERSION = "v0.1.0"

var cli struct {
	Logging string      `help:"Logging verbosity." enum:"info,debug,trace" short:"l" default:"info"`
	Version VersionFlag `help:"Print version information and quit" name:"version" short:"v"`
	Import  struct {
		Input string `help:"The input file. Either .osm or .osm.pbf." placeholder:"<input-file>" arg:"" type:"existingfile"`
	} `cmd:"" help:"Imports the given OSM file to use it in queries."`
	Query struct {
		Query string `help:"The query string." placeholder:"<query>" arg:""`
	} `cmd:"" help:"Returns the OSM data for the given query."`
}

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

	switch ctx.Command() {
	case "import <input>":
		importing.Import(cli.Import.Input)
	case "query <query>":
		tagIndex, err := index.LoadTagIndex()
		sigolo.FatalCheck(err)

		_, err = query.ParseQueryString(`
// this is a comment
bbox(1,2,3,4).nodes{ (amenity=bench OR height=10.5) AND backrest=no }
`, tagIndex)
		sigolo.FatalCheck(err)
		//query.ParseQueryString(`// this is a comment
		//
		//bbox(1,  2,3,
		//4).nodes{   amenity
		//=bench}`)
	default:
		sigolo.Errorf("Unknown command '%s'", ctx.Command())
	}
}
