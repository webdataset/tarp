package datapipes

import "log"

// CopySamples unconditionally copies samples from inch to outch.
func CopySamples(inch Pipe, outch Pipe) {
	for sample := range inch {
		outch <- sample
	}
	close(outch)
}

// SliceSamples takes a slice out of a pipe (from start to end).
func SliceSamples(start, end int) func(inch Pipe, outch Pipe) {
	return func(inch Pipe, outch Pipe) {
		count := 0
		for sample := range inch {
			if count < start {
				continue
			}
			if end >= 0 && count >= end {
				break
			}
			outch <- sample
			count++
		}
		Debug.Println("SliceSamples end")
		close(outch)
	}
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
