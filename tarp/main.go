package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/tmbdev/tarp/dpipes"
)

var opts struct {
	Verbose bool   `short:"v" description:"verbose output"`
	Errlog  string `long:"errlog" default:"stderr"`
	Infolog string `long:"infolog" default:"stderr"`
}

// Commands is a registry of functions implementing subcommands.
var Commands map[string]func() = make(map[string]func())

// Parser is the main command line parser; add with AddCommand.
var Parser = flags.NewParser(&opts, flags.Default)

var infolog *log.Logger
var errlog *log.Logger

// Handle errors.
func Handle(err error) {
	if err != nil {
		panic(err)
	}
}

// Validate the boolean expression and exit if not satisfied.
func Validate(ok bool, args ...interface{}) {
	if ok {
		return
	}
	result := make([]string, len(args))
	for i, v := range args {
		result[i] = fmt.Sprintf("%v", v)
	}
	message := strings.Join(result, " ")
	fmt.Println("Error:", message)
	os.Exit(1)
}

func main() {
	infolog = dpipes.OpenLogger("stderr", "info")
	errlog = dpipes.OpenLogger("stderr", "error")
	if len(os.Args) == 1 {
		Parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}
	_, err := Parser.Parse()
	if err != nil {
		flagsErr, ok := err.(*flags.Error)
		if ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
	if Parser.Active == nil {
		Parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}
	Commands[Parser.Active.Name]()
}
