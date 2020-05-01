package main

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/tmbdev/tarp/dpipes"
)

var splitopts struct {
	Count      int     `short:"c" long:"count" description:"max count per shard" default:"1000000"`
	Size       float64 `short:"s" long:"size" description:"max size per shard" default:"1e9"`
	Pattern    string  `short:"o" long:"output" description:"output pattern" default:"split-%06d.tar"`
	Post       string  `short:"p" long:"post" description:"command running after each shard; use %s for shard file"`
	Start      int     `long:"start" description:"start for slicing" default:"0"`
	End        int     `long:"end" description:"end for slicing" default:"-1"`
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

func splitcmd() {
	Validate(strings.Contains(splitopts.Pattern, "%"), "pattern must contain something like %06d")
	Validate(splitopts.Count >= 2, "count must be >= 2")
	Validate(splitopts.Size >= 1e3, "size must be >= 1e3")
	var post func(string)
	if splitopts.Post != "" {
		post = func(name string) {
			cmd := fmt.Sprintf(splitopts.Post, name)
			proc := exec.Command("/bin/bash", "-c", cmd)
			proc.Run()
		}
	}
	dpipes.Processing(
		dpipes.TarSources(splitopts.Positional.Inputs, nil),
		dpipes.SliceSamples(splitopts.Start, catopts.End),
		dpipes.ShardingTarSink(
			splitopts.Count,
			int(splitopts.Size),
			splitopts.Pattern,
			post),
	)
}

func init() {
	Parser.AddCommand("split", "split tar files", "", &splitopts)
	Commands["split"] = splitcmd
}
