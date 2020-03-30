package main

import (
	"regexp"
	"strings"

	"github.com/tmbdev/tarp/dpipes"
)

var catopts struct {
	Fields  string `short:"f" long:"field" description:"fields to extract"`
	Output  string `short:"o" long:"outputs" description:"output file"`
	Start   int    `long:"start" description:"start for slicing" default:"0"`
	End     int    `long:"end" description:"end for slicing" default:"-1"`
	Shuffle int    `short:"s" long:"shuffle" description:"shuffle samples in memory" default:"0"`
	Noeof   bool   `short:"E" long:"noeof" description:"don't send/receive EOF for ZMQ"`
	Logging int    `short:"L" long:"logging" description:"log this often" default:"0"`
	// Shuffle int
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

var zurlre *regexp.Regexp = regexp.MustCompile("^z[a-z]*:")

func makesource(inputs []string, eof bool) func(dpipes.Pipe) {
	if zurlre.MatchString(inputs[0]) {
		Validate(len(inputs) == 1, "can only use a single ZMQ url for input")
		infolog.Println("# makesource (ZMQ)", inputs[0])
		return dpipes.ZMQSource(inputs[0], eof)
	}
	infolog.Println("# makesource", inputs)
	return dpipes.TarSources(inputs)
}

func makesink(output string, eof bool) func(dpipes.Pipe) {
	if zurlre.MatchString(output) {
		infolog.Println("# makesink (ZMQ)", output)
		return dpipes.ZMQSink(output, eof)
	}
	infolog.Println("# makesink ", output)
	return dpipes.TarSinkFile(output)
}

func catcmd() {
	Validate(len(catopts.Positional.Inputs) >= 1, "must provide at least one input (can be '-')")
	Validate(catopts.Output != "", "must provide output (can be '-')")
	infolog.Println("#", catopts.Positional.Inputs)
	infolog.Println("#", catopts.Start, catopts.End)
	processes := make([]dpipes.Process, 0, 100)
	processes = append(processes, dpipes.SliceSamples(catopts.Start, catopts.End))
	if catopts.Logging > 0 {
		infolog.Println("# logging", catopts.Logging)
		processes = append(processes,
			dpipes.LogProgress(
				"cat", catopts.Logging, infolog,
			),
		)
	}
	if catopts.Fields != "" {
		fields := strings.Split("__key__ "+catopts.Fields, " ")
		infolog.Println("# rename", fields)
		processes = append(processes, dpipes.RenameSamples(fields, false))
	}
	if catopts.Shuffle > 0 {
		n := catopts.Shuffle
		infolog.Println("# shuffle", n)
		processes = append(processes, dpipes.Shuffle(n+1, n/2+1))
	}
	dpipes.Debug.Println("catcmd", dpipes.MyInfo())
	dpipes.Processing(
		makesource(catopts.Positional.Inputs, !catopts.Noeof),
		dpipes.Pipeline(processes...),
		makesink(catopts.Output, !catopts.Noeof),
	)
}

func init() {
	Parser.AddCommand("cat", "concatenate tar files", "", &catopts)
	Commands["cat"] = catcmd
}
