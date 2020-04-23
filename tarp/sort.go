package main

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmbdev/tarp/dpipes"
	dpipesSQL "github.com/tmbdev/tarp/dpipes/sql"
)

var sortopts struct {
	Fields     string `short:"f" long:"field" description:"fields to extract; name or name:old1;old2;old3"`
	Output     string `short:"o" long:"outputs" description:"output file"`
	Sortfields string `short:"s" long:"sortfield" description:"fields to sort on" default:"RANDOM()"`
	Start      int    `long:"start" description:"start for slicing" default:"0"`
	End        int    `long:"end" description:"end for slicing" default:"-1"`
	Tmpdir     string `long:"tmpdir" description:"temporary storage for sorting" default:""`
	Keepdb     bool   `long:"keepdb"`
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

func sortcmd() {
	Validate(len(sortopts.Positional.Inputs) > 0, "must provide at least one input (can be '-')")
	Validate(sortopts.Fields != "", "you must provide some fields")
	Validate(sortopts.Output != "", "must provide output (can be '-')")
	fields := strings.Split("__key__ "+sortopts.Fields, " ")
	sortfields := strings.Split(sortopts.Sortfields, " ")
	Validate(len(sortfields) == 1, "only one sort field can be specified")
	infolog.Println("# inputs", sortopts.Positional.Inputs)
	infolog.Println("# start:end", sortopts.Start, sortopts.End)
	dbdir := ""
	if sortopts.Tmpdir != "" {
		dbdir = sortopts.Tmpdir
	} else {
		s, err := ioutil.TempDir("", "dbdir")
		Handle(err)
		defer os.RemoveAll(dbdir)
		dbdir = s
	}
	dbname := filepath.Join(dbdir, "__tarp_sort__.db")
	db, err := sql.Open("sqlite3", dbname)
	Handle(err)
	defer db.Close()
	defer os.Remove(dbname)
	tname := "samples"
	infolog.Println("writing")
	dpipes.Processing(
		dpipes.TarSources(sortopts.Positional.Inputs),
		dpipes.SliceSamples(sortopts.Start, sortopts.End),
		dpipesSQL.DBSink(db, tname, fields),
	)
	infolog.Println("reading")
	dpipes.Processing(
		dpipesSQL.DBSource(db, tname, fields, sortfields[0]),
		dpipes.CopySamples,
		dpipes.TarSinkFile(sortopts.Output),
	)
}

func init() {
	Parser.AddCommand("sort", "sort tar files", "", &sortopts)
	Commands["sort"] = sortcmd
}
