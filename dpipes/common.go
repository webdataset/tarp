package dpipes

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
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

// Default pipe size for most internal pipes
var Pipesize int

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

// Handle is a generic warning handler (returns after printing warning)
func Warn(err error, args ...interface{}) {
	if err != nil {
		result := make([]string, len(args))
		for i, v := range args {
			result[i] = fmt.Sprintf("%v", v)
		}
		message := strings.Join(result, " ")
		fmt.Println("Catch:", message)
	}
}

// Assert checks the assertion and panics with message if fails.
func Assert(ok bool, args ...interface{}) error {
	if ok {
		return nil
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
func GetFirst(sample Sample, spec string) (Bytes, string, error) {
	for _, field := range strings.Split(spec, ",") {
		if value, ok := sample[field]; ok {
			return value, field, nil
		}
	}
	return Bytes{}, "", errors.New(spec + ": not found")
}

// expand {000..123} notation in strings (similar to shell)
func ExpandBraces(s string) []string {
	pattern := regexp.MustCompile("[{][0-9]+[.][.][0-9]+[}]")
	loc := pattern.FindStringIndex(s)
	if len(loc) == 0 {
		return []string{s}
	}
	sub := s[loc[0]+1 : loc[1]-1]
	lohi := strings.Split(sub, "..")
	lo, _ := strconv.Atoi(lohi[0])
	hi, _ := strconv.Atoi(lohi[1])
	prefix := s[:loc[0]]
	rest := ExpandBraces(s[loc[1]:])
	result := make([]string, 0, 100)
	for i := lo; i <= hi; i++ {
		for _, s := range rest {
			expanded := fmt.Sprintf("%s%0*d%s", prefix, len(lohi[0]), i, s)
			result = append(result, expanded)
		}
	}
	return result
}

// OpenLogger opens a logger on a given file,
// with abbreviations for stdout/stderr
func OpenLogger(where string, ident string) *log.Logger {
	where = GetEnv(ident+"_log", where)
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
	Debug = OpenLogger("", "debug")
	Progress = OpenLogger("stderr", "progress")
	size, err := strconv.Atoi(GetEnv("TARP_PIPESIZE", "10"))
	Handle(err)
	Assert(size > 1, "pipesize too small")
	Assert(size < 10000, "pipesize too large")
	Pipesize = size
}
