package main

import (
	"bufio"
	"io"
	"regexp"

	"github.com/tmbdev/tarp/dpipes"
)

var createopts struct {
	Output     string `short:"o" long:"output" description:"output file" default:""`
	Count      int    `long:"count" description:"maximum number of files to write (for testing)" default:"-1"`
	Positional struct {
		Input string `required:"yes"`
	} `positional-args:"yes"`
}

func createcmd() {
	Validate(createopts.Output != "", "must give output with -o (can be '-')")
	source, err := dpipes.GOpen(createopts.Positional.Input)
	Handle(err)
	defer source.Close()
	reader := bufio.NewReader(source)
	whitespace := regexp.MustCompile("\\s+")
	lineno := 0
	outch := make(dpipes.RawPipe)
	done := dpipes.WaitFor(func() {
		stream, err := dpipes.GCreate(createopts.Output)
		Handle(err)
		defer stream.Close()
		dpipes.TarRawSink(stream)(outch)
	})
	count := 0
	for {
		if createopts.Count >= 0 && count >= createopts.Count {
			infolog.Println(count, "[COUNT]")
			break
		}
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			infolog.Println(count, "[EOF]")
			break
		}
		Handle(err)
		lineno += 1
		fields := whitespace.Split(string(line), 2)
		Validate(len(fields) == 2, "bad input line at", lineno, ":", line)
		infolog.Println(count, fields[0], "<-", fields[1])
		output, source := fields[0], fields[1]
		contents, err := dpipes.ReadBinary(source)
		Handle(err)
		outch <- dpipes.Raw{Key: output, Value: contents}
		count++
	}
	close(outch)
	<-done
}

func init() {
	Parser.AddCommand("create", "create tar files from recipes", "", &createopts)
	Commands["create"] = createcmd
}
