package dpipes

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Bytes is a shorthand for array of bytes.
type Bytes []byte

// Source functions push samples into pipes.
type Source func(Pipe)

// Sink functions consume samples from pipes.
type Sink func(Pipe)

// Process takes samples from a pipe (first
// argument), processes it, and pushes it onto
// the output pipe (second argument)
type Process func(Pipe, Pipe)

// Sample is a map representing related values in data processing.
type Sample map[string]Bytes

// SampleF is a function on samples, with error output
type SampleF func(Sample) (Sample, error)

// Pipe is channel of samples.
type Pipe chan Sample

// Debug is a logger for debugging messages
var Debug *log.Logger

// Progress is a logger for reporting progress
var Progress *log.Logger

// Handle is a generic error handler (usually calls panic)
func Handle(err error, args ...interface{}) {
	if err != nil {
		result := make([]string, len(args))
		for i, v := range args {
			result[i] = fmt.Sprintf("%v", v)
		}
		message := strings.Join(result, " ")
		fmt.Println("Catch:", message)
		panic(err)
	}
}

// Assert checks the assertion and panics with message if fails.
func Assert(ok bool, args ...interface{}) {
	if ok {
		return
	}
	result := make([]string, len(args))
	for i, v := range args {
		result[i] = fmt.Sprintf("%v", v)
	}
	message := strings.Join(result, " ")
	fmt.Println("Assert:", message)
	panic(errors.New(message))
}

// GetEnv looks up an environment variable with default.
func GetEnv(key, dflt string) string {
	value, present := os.LookupEnv(key)
	if present {
		return value
	}
	return dflt
}

// StrSample converts samples to readable strings.
func StrSample(sample Sample) string {
	result := ""
	for k, v := range sample {
		s := strconv.QuoteToASCII(string(v))
		if len(s) > 50 {
			s = s[:50]
		}
		result += fmt.Sprintf(" | %-20s %s\n", k, s)
	}
	return result
}

// GetFirst gets the first key matching the spec
func GetFirst(sample Sample, spec string) (Bytes, error) {
	for _, field := range strings.Split(spec, ",") {
		if value, ok := sample[field]; ok {
			return value, nil
		}
	}
	return Bytes{}, errors.New(spec + ": not found")
}

// OpenLogger opens a logger on a given file,
// with abbreviations for stdout/stderr
func OpenLogger(where string, ident string) *log.Logger {
	prefix := "[" + ident + "] "
	if where == "null" || where == "" {
		stream, _ := os.Open("/dev/null")
		return log.New(stream, prefix, 0)
	}
	if where == "stderr" {
		return log.New(os.Stderr, prefix, 0)
	}
	if strings.Contains(where, "/") {
		stream, err := os.Create(where)
		Handle(err)
		return log.New(stream, prefix, 0)
	}
	panic(errors.New(where + ": bad log dest"))
}

// MyInfo gets current goroutine info.
func MyInfo() string {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = b[:bytes.IndexByte(b, ':')]
	return string(b)
}

func init() {
	Debug = OpenLogger(GetEnv("debug", ""), "debug")
	Progress = OpenLogger(GetEnv("progress", "stderr"), "progress")
}
