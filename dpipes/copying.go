package dpipes

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// CopySamples unconditionally copies samples from inch to outch.
func CopySamples(inch Pipe, outch Pipe) {
	for sample := range inch {
		outch <- sample
	}
	close(outch)
}

// RekeySamples changes the sample key based on a spec
func RekeySamples(spec string) func(inch Pipe, outch Pipe) {
	Assert(spec == "#", "only '#' supported as spec right now")
	return func(inch Pipe, outch Pipe) {
		count := 0
		for sample := range inch {
			sample["__key__"] = Bytes(fmt.Sprintf("%09d", count))
			outch <- sample
			count++
		}
		Debug.Println("SliceSamples end")
		close(outch)
	}
}

// SliceSamples takes a slice out of a pipe (from start to end).
func SliceSamplesStep(start, end, step int) func(inch Pipe, outch Pipe) {
	return func(inch Pipe, outch Pipe) {
		Debug.Println("SliceSamples start", start, end, step)
		count := 0
		for sample := range inch {
			if count < start {
				count++
				continue
			}
			if end >= 0 && count >= end {
				break
			}
			if count%step == 0 {
				outch <- sample
			}
			count++
		}
		Debug.Println("SliceSamples end")
		close(outch)
	}
}

// SliceSamples takes a slice out of a pipe (from start to end), using step==1
func SliceSamples(start, end int) func(inch Pipe, outch Pipe) {
	return SliceSamplesStep(start, end, 1)
}

// ParseSliceSpec parses a lo:hi:step-style spec
func ParseSliceSpec(spec string) (int, int, int) {
	steps := strings.Split(spec, ":")
	Assert(len(steps) >= 1 && len(steps) <= 3, spec, "must be lo:hi or lo:hi:step")
	lo := 0
	hi := 999999999
	st := 1
	var err error
	if len(steps) == 1 {
		lo, err = strconv.Atoi(steps[0])
		Handle(err)
		hi = lo + 1
	} else {
		if steps[0] != "" {
			lo, err = strconv.Atoi(steps[0])
			Handle(err)
		}
		if steps[1] != "" {
			hi, err = strconv.Atoi(steps[1])
			Handle(err)
		}
		if len(steps) > 2 {
			st, err = strconv.Atoi(steps[2])
			Handle(err)
		}
	}
	return lo, hi, st
}

// SliceSamples takes a slice out of a pipe using spec=="lo:hi:step"
func SliceSamplesSpec(spec string) func(inch Pipe, outch Pipe) {
	if spec == "" {
		return CopySamples
	}
	lo, hi, st := ParseSliceSpec(spec)
	return SliceSamplesStep(lo, hi, st)
}

// LogProgress displays processing progress. Info must contain %d and %s,
// in that order.
func LogProgress(info string, every int, where *log.Logger) func(Pipe, Pipe) {
	return func(inch Pipe, outch Pipe) {
		Debug.Println("LogProgress starting")
		count := 0
		for sample := range inch {
			if count%every == 0 {
				key := sample["__key__"]
				where.Printf(info, count, string(key))
			}
			outch <- sample
			count++
		}
		close(outch)
		Debug.Println("LogProgress finished")
	}
}
