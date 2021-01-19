/*
Package dpipes provides filters and utilities
for processing samples in pipelines, in particular tar
archives containing data for deep learning and
big data applications.

Many functions in this package are sources, sinks,
or filters for pipelines. These functions are
generally intended to be invoked as goroutines.

The convention is that the functions take input channel(s)
first and output channel(s) second.

Channel filters are generally curried, meaning they are
called in a form like MyFilter(param)(inch, outch)
*/
package dpipes

import (
	"fmt"
	"io"
)

// WaitFor runs the given function in a new goroutine and sends
// "done" to the output channel.
// You can wait for the function to finish using:
// done := WaitFor(f); <-done
func WaitFor(f func()) chan string {
	done := make(chan string, 2)
	go func() { f(); done <- "done" }()
	return done
}

// Rename renames fields in a sample.
func (sample *Sample) Rename(from, to string) *Sample {
	(*sample)[to] = (*sample)[from]
	delete(*sample, from)
	return sample
}

// SampleSize estimates the size of a sample in bytes
func SampleSize(sample Sample) int {
	total := 0
	for k, v := range sample {
		total += len(k)
		total += len(v)
	}
	return total
}

// TarSource reads a tar file and outputs a stream of samples.
func TarSource(stream io.ReadCloser) func(Pipe) {
	return func(outch Pipe) {
		rawinch := make(RawPipe, Pipesize)
		go Aggregate(rawinch, outch)
		TarRawSource(stream)(rawinch)
		stream.Close()
	}
}

// TarSourceFile reads a tar file and outputs a stream of samples.
func TarSourceFile(fname string) func(Pipe) {
	stream, err := GOpen(fname)
	if err != nil {
		panic(err)
	}
	return TarSource(stream)
}

// CountSamples returns the number of samples in the channel.
// It's mainly used for testing.
func CountSamples(inch Pipe) int {
	count := 0
	for range inch {
		count++
	}
	return count
}

func deletechannel(channels []Pipe, i int) []Pipe {
	if len(channels) < 1 {
		return []Pipe{}
	}
	for j := i + 1; j < len(channels); j++ {
		channels[j-1] = channels[j]
	}
	return channels[:len(channels)-1]
}

// TarMixer opens #group files simultaneously and mixes them
// into an output channel in a round-robin way.
func TarMixer(urls []string, group int, csize int, proc func() Process) func(Pipe) {
	return func(outch Pipe) {
		sources := make([]Pipe, 0, group)
		Debug.Println("tarmixer", len(urls), "group", group)
		for i := 0; ; i++ {
			for len(urls) > 0 && len(sources) < group {
				Debug.Println("tarmixer open", urls[0])
				c := make(Pipe, csize)
				go TarSourceFile(urls[0])(c)
				if proc != nil {
					c2 := make(Pipe, csize)
					go proc()(c, c2)
					sources = append(sources, c2)
				} else {
					sources = append(sources, c)
				}
				urls = urls[1:]
				Debug.Println("tarmixer remaining", urls)
			}
			if len(sources) == 0 {
				break
			}
			index := i % len(sources)
			sample, more := <-sources[index]
			if more {
				outch <- sample
			} else {
				Debug.Println("tarmixer input", index, "done", len(sources))
				sources = deletechannel(sources, index)
			}
		}
		Debug.Println("tarmixer all done")
		close(outch)
	}
}

// TarSources opens and reads multiple tar files
// and sends their output to the pipe.
func TarSources(urls []string, proc func() Process) func(Pipe) {
	return func(outch Pipe) {
		Debug.Println("TarSources", urls)
		sources := make(chan Pipe, Pipesize)
		go CombinePipes(sources, outch)
		for _, url := range urls {
			Progress.Println("# source", url)
			temp := make(Pipe, Pipesize)
			if proc != nil {
				temp2 := make(Pipe, Pipesize)
				go proc()(temp, temp2)
				sources <- temp2
			} else {
				sources <- temp
			}
			TarSourceFile(url)(temp)
		}
		close(sources)
		Debug.Println("TarSources done")
	}
}

// TarSink writes a stream of samples to disk as a tar file.
func TarSink(stream io.WriteCloser) func(Pipe) {
	return func(inch Pipe) {
		rawinch := make(RawPipe, Pipesize)
		go Disaggregate(inch, rawinch)
		TarRawSink(stream)(rawinch)
		stream.Close()
	}
}

// TarSinkFile writes a stream to a file (GOpen).
func TarSinkFile(fname string) func(Pipe) {
	Progress.Println("# writing", fname)
	stream, err := GCreate(fname)
	if err != nil {
		panic(err)
	}
	return TarSink(stream)
}

// ShardingTarSink takes samples and splits them up across multiple
// shards, respecting sample boundaries.
func ShardingTarSink(maxcount, maxsize int, pattern string, callback func(string)) func(Pipe) {
	return func(inch Pipe) {
		count := 0
		shards := make(chan Pipe, Pipesize)
		go MakeShards(maxcount, maxsize)(inch, shards)
		for inch := range shards {
			name := fmt.Sprintf(pattern, count)
			Progress.Println("# shard", name)
			count++
			stream, _ := GCreate(name)
			TarSink(stream)(inch)
			if callback != nil {
				callback(name)
			}
		}
	}
}
