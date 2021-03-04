package dpipes

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Raw is a struct representing unaggregated data items (e.g., from a tar file).
type Raw struct {
	Key   string
	Value Bytes
}

// RawPipe is a channel consisting of Raw data items.
type RawPipe chan Raw

var pattern string = "^((?:.*/)?(?:[^/.]+))[.]?([^/]*)$"
var patternRe *regexp.Regexp
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
	var out Sample
	for sample := range inch {
		key, suffix := FnameSplit(sample.Key)
		if key != lastKey {
			if out != nil {
				outch <- out
			}
			out = make(Sample)
			lastKey = key
			out["__key__"] = []byte(key)
		}
		out[suffix] = sample.Value
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
	count := 0
	for sample := range inch {
		if len(sample) == 0 {
			continue
		}
		prefix := string(sample["__key__"])
		// Debug.Println("Disaggregate", prefix)
		if prefix == "" {
			prefix = fmt.Sprintf("_%09d_", count)
		}
		Assert(prefix != "", "encountered empty prefix")
		for key, value := range sample {
			if key == "" {
				continue
			}
			if key[0] == "_"[0] {
				continue
			}
			outch <- Raw{FnameCombine(prefix, key), value}
		}
		count++
	}
	close(outch)
}

var TarHandler func(error) = func(err error) { Handle(err) }

// TarRawSource extracts and loads files from a tar archive and
// send them to the channel as a raw key/value pair.
func TarRawSource(stream io.Reader) func(RawPipe) {
	return func(outch RawPipe) {
		tr := tar.NewReader(stream)
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				TarHandler(err)
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
			Debug.Println("# tar write", sample.Key, MyInfo())
			var header tar.Header
			header.Name = sample.Key
			header.Size = int64(len(sample.Value))
			header.Mode = 0755
			header.Uid = 1000
			header.Gid = 1000
			header.Uname = "bigdata"
			header.Gname = "bigdata"
			header.ModTime = time.Now().Add(time.Second * (-2))
			header.AccessTime = header.ModTime
			header.ChangeTime = header.ModTime
			if err := tr.WriteHeader(&header); err != nil {
				panic(err)
			}
			samplestream := bytes.NewReader(sample.Value)
			_, err := io.Copy(tr, samplestream)
			if err != nil {
				panic(err)
			}
		}
		tr.Close()
		// FIXME close stream here (analogous to channels)
	}
}

// ShardingRawSink splits up a stream of inputs by size and count and
// invokes a callback for each shard.
func ShardingRawSink(maxcount, maxsize int) func(RawPipe, chan RawPipe) {
	return func(inch RawPipe, outch chan RawPipe) {
		var current RawPipe
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
			size += len(sample.Value)
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
		shards := make(chan RawPipe, Pipesize)
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
