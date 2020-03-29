package datapipes

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"regexp"
)

// Raw is a struct representing unaggregated data items (e.g., from a tar file).
type Raw struct {
	key   string
	value Bytes
}

// RawPipe is a channel consisting of Raw data items.
type RawPipe chan Raw

var pattern string = "^((?:.*/)?(?:[^/.]+))[.]?([^/]*)$"
var patternRe *regexp.Regexp = nil
var combiner string = "."

// FnameSplit is used for for aggregating/disaggregating
// a sorted list of file into groups of related files sharing
// a common basename.
func FnameSplit(s string) (key, suffix string) {
	if patternRe == nil {
		re, _ := regexp.Compile(pattern)
		patternRe = re
	}
	matches := patternRe.FindStringSubmatch(s)
	key = matches[1]
	suffix = matches[2]
	return
}

// FnameCombine combines a prefix and suffix back into
// a tar file name.
func FnameCombine(key, suffix string) string {
	return key + combiner + suffix
}

// Aggregate aggregates raw samples into maps. Each map maps the
// file name extension to the contents of the file.
// The common basenamen is stored in __key__.
func Aggregate(inch RawPipe, outch Pipe) {
	Debug.Println("Aggregate")
	lastKey := ""
	var out Sample = nil
	for sample := range inch {
		key, suffix := FnameSplit(sample.key)
		if key != lastKey {
			if out != nil {
				outch <- out
			}
			out = make(Sample)
			lastKey = key
			out["__key__"] = []byte(key)
		}
		out[suffix] = sample.value
	}
	if out != nil {
		outch <- out
	}
	Debug.Println("Aggregate closing", outch)
	close(outch)
	Debug.Println("Aggregate done")
}

// Disaggregate is the inverse of Aggregate.
func Disaggregate(inch Pipe, outch RawPipe) {
	for sample := range inch {
		prefix := string(sample["__key__"])
		// Debug.Println("Disaggregate", prefix)
		Assert(prefix != "", "encountered empty prefix")
		for key, value := range sample {
			if key[0] == "_"[0] {
				continue
			}
			outch <- Raw{FnameCombine(prefix, key), value}
		}
	}
	close(outch)
}

// TarRawSource extracts and loads files from a tar archive and
// send them to the channel as a raw key/value pair.
func TarRawSource(stream io.Reader) func(RawPipe) {
	return func(outch RawPipe) {
		tr := tar.NewReader(stream)
		for {
			header, err := tr.Next()
			if header == nil {
				break
			}
			if header.Typeflag != tar.TypeReg {
				continue
			}
			if err != nil {
				panic(err)
			}
			var buffer bytes.Buffer
			io.Copy(&buffer, tr)
			data := buffer.Bytes()
			sample := Raw{header.Name, data}
			outch <- sample
		}
		close(outch)
	}
}

// TarRawSink takes raw key value pairs from a channel and
// write them into a tar archive.
func TarRawSink(stream io.Writer) func(RawPipe) {
	return func(inch RawPipe) {
		tr := tar.NewWriter(stream)
		for sample := range inch {
			Debug.Println("# tar write", sample.key, MyInfo())
			var header tar.Header
			header.Name = sample.key
			header.Size = int64(len(sample.value))
			header.Mode = 0755
			header.Uid = 1000
			header.Gid = 1000
			header.Uname = "bigdata"
			header.Gname = "bigdata"
			if err := tr.WriteHeader(&header); err != nil {
				panic(err)
			}
			samplestream := bytes.NewReader(sample.value)
			_, err := io.Copy(tr, samplestream)
			if err != nil {
				panic(err)
			}
		}
		tr.Close()
	}
}

// ShardingRawSink splits up a stream of inputs by size and count and
// invokes a callback for each shard.
func ShardingRawSink(maxcount, maxsize int) func(RawPipe, chan RawPipe) {
	return func(inch RawPipe, outch chan RawPipe) {
		var current RawPipe = nil
		size := 0
		count := 0
		for sample := range inch {
			if count >= maxcount || size >= maxsize {
				close(current)
				current = nil
				count = 0
				size = 0
			}
			if current == nil {
				current = make(RawPipe)
				outch <- current
			}
			current <- sample
			count++
			size += len(sample.value)
		}
		close(current)
		close(outch)
	}
}

// ShardingRawTarSink takes Raw items and splits them up across multiple
// shards.
func ShardingRawTarSink(maxcount, maxsize int, pattern string, callback func(string)) func(RawPipe) {
	return func(inch RawPipe) {
		count := 0
		shards := make(chan RawPipe, 100)
		go ShardingRawSink(maxcount, maxsize)(inch, shards)
		for inch := range shards {
			name := fmt.Sprintf(pattern, count)
			count++
			stream, _ := GCreate(name)
			TarRawSink(stream)(inch)
			stream.Close()
			if callback != nil {
				callback(name)
			}
		}
	}
}
