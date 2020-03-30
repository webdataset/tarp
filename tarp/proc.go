package main

import (
	"strings"

	"github.com/tmbdev/tarp/dpipes"
)

var procopts struct {
	Fields     string `short:"f" long:"field" description:"fields to extract; name or name=old1,old2,old3"`
	Output     string `short:"o" long:"outputs" description:"output file"`
	Start      int    `long:"start" description:"start for slicing" default:"0"`
	End        int    `long:"end" description:"end for slicing" default:"-1"`
	Command    string `short:"c" long:"command" description:"shell command running in each sample dir"`
	Shell      string `long:"shell" description:"shell command running in each sample dir" default:"/bin/bash"`
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

func proccmd() {
	Validate(len(procopts.Positional.Inputs) >= 1, "must provide at least one input (can be '-')")
	Validate(procopts.Output != "", "must provide output (can be '-')")
	infolog.Println("#", procopts.Positional.Inputs)
	infolog.Println("#", procopts.Start, procopts.End)
	processes := make([]dpipes.Process, 0, 100)
	processes = append(processes, dpipes.SliceSamples(procopts.Start, procopts.End))
	if procopts.Fields != "" {
		fields := strings.Split("__key__ "+procopts.Fields, " ")
		infolog.Println("# rename", fields)
		processes = append(processes, dpipes.RenameSamples(fields, false))
	}
	if procopts.Command != "" {
		processes = append(processes, dpipes.ProcessSamples(procopts.Command, "/tmp", false))
	}
	dpipes.Processing(
		makesource(procopts.Positional.Inputs, true),
		dpipes.Pipeline(processes...),
		makesink(procopts.Output, true),
	)
}

func init() {
	Parser.AddCommand("proc", "process tar files", "", &procopts)
	Commands["proc"] = proccmd
}
