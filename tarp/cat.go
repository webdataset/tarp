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
	Fields     string `short:"f" long:"field" description:"fields to extract"`
	Output     string `short:"o" long:"outputs" description:"output file"`
	Shuffle    int    `short:"s" long:"shuffle" description:"shuffle samples in memory" default:"0"`
	Noeof      bool   `short:"E" long:"noeof" description:"don't send/receive EOF for ZMQ"`
	Logging    int    `short:"L" long:"logging" description:"log this often" default:"0"`
	Mix        int    `short:"m" long:"mix" description:"mix samples from multiple sources" default:"0"`
	Load       bool   `short:"l" long:"load" description:"load file list"`
	Maxerr     int    `long:"maxerr" description:"maximum number of errors" default:"0"`
	ShardSlice string `long:"shardslice" description:"select samples from each input"`
	Slice      string `long:"slice" description:"select samples (lo:hi:step)"`
	Rekey      string `short:"R" long:"rekey" description:"replace the key based on spec (--rekey='#')"`
	TarNoErr   bool   `long:"tarnoerr" description:"ignore errors in tar"`
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
	if catopts.TarNoErr {
		dpipes.TarHandler = func(err error) { dpipes.Warn(err) }
	}
	if zurlre.MatchString(inputs[0]) {
		Validate(len(inputs) == 1, "can only use a single ZMQ url for input")
		verbose.Println("# makesource (ZMQ)", inputs[0])
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
	verbose.Println("# got", len(urls), "inputs")
	var selector func() dpipes.Process = nil
	if catopts.ShardSlice != "" {
		selector = func() dpipes.Process {
			return dpipes.SliceSamplesSpec(catopts.ShardSlice)
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
	verbose.Println("# makesink ", output)
	return dpipes.TarSinkFile(output)
}

func catcmd() {
	Validate(len(catopts.Positional.Inputs) >= 1, "must provide at least one input (can be '-')")
	Validate(catopts.Output != "", "must provide output (can be '-')")
	verbose.Println("#", catopts.Positional.Inputs)
	verbose.Println("#", catopts.Slice, catopts.ShardSlice)
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
	processes = append(processes, dpipes.SliceSamplesSpec(catopts.Slice))
	if catopts.Logging > 0 {
		verbose.Println("# logging", catopts.Logging)
		processes = append(processes,
			dpipes.LogProgress(
				"cat", catopts.Logging, infolog,
			),
		)
	}
	if catopts.Fields != "" {
		fields := strings.Split("__key__ "+catopts.Fields, " ")
		verbose.Println("# rename", fields)
		processes = append(processes, dpipes.RenameSamples(fields, false))
	}
	if catopts.Shuffle > 0 {
		n := catopts.Shuffle
		infolog.Println("# shuffle", n)
		processes = append(processes, dpipes.Shuffle(n+1, n/2+1))
	}
	if catopts.Rekey != "" {
		processes = append(processes, dpipes.RekeySamples(catopts.Rekey))
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
