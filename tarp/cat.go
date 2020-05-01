package main

import (
	"bufio"
	"io"
	"os"
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
	Mix     int    `short:"m" long:"mix" description:"mix samples from multiple sources" default:"0"`
	Load    bool   `short:"l" long:"load" description:"load file list"`
	Maxerr  int    `long:"maxerr" description:"maximum number of errors" default:"0"`
	Select  string `long:"select" description:"select samples from input"`
	// Shuffle int
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

var zurlre *regexp.Regexp = regexp.MustCompile("^z[a-z]*:")

func readlines(fname string) []string {
	source, err := os.Open(fname)
	Handle(err)
	defer source.Close()
	reader := bufio.NewReader(source)
	result := make([]string, 0, 100)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		Handle(err)
		result = append(result, strings.TrimSpace(string(line)))
	}
	return result
}

func makesource(inputs []string, eof bool) func(dpipes.Pipe) {
	if zurlre.MatchString(inputs[0]) {
		Validate(len(inputs) == 1, "can only use a single ZMQ url for input")
		infolog.Println("# makesource (ZMQ)", inputs[0])
		return dpipes.ZMQSource(inputs[0], eof)
	}
	if catopts.Load {
		Validate(len(inputs) == 1, "use --load only with single file")
		inputs = readlines(inputs[0])
	}
	urls := make([]string, 0, 100)
	for _, source := range inputs {
		expanded := dpipes.ExpandBraces(source)
		urls = append(urls, expanded...)
	}
	infolog.Println("# got", len(urls), "inputs")
	var selector func() dpipes.Process = nil
	if catopts.Select != "" {
		selector = func() dpipes.Process {
			return dpipes.SliceSamplesSpec(catopts.Select)
		}
	}
	if catopts.Mix > 0 {
		return dpipes.TarMixer(urls, catopts.Mix, 100, selector)
	} else {
		return dpipes.TarSources(urls, selector)
	}
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
	if catopts.Maxerr > 0 {
		nerrors := 0
		dpipes.TarHandler = func(err error) {
			if nerrors >= catopts.Maxerr {
				panic(err)
			}
			errlog.Println("WARNING", err)
			nerrors++
		}
	}
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
