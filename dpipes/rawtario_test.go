package dpipes

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var patterns = []struct {
	in     string
	key    string
	suffix string
}{
	{"http://a/b/c/d/e.f.g", "http://a/b/c/d/e", "f.g"},
	{"d/e.f.g", "d/e", "f.g"},
	{"e.f.g", "e", "f.g"},
	{"e.f", "e", "f"},
	{"e", "e", ""},
}

func TestFnameSplit(t *testing.T) {
	for _, tc := range patterns {
		key, suffix := FnameSplit(tc.in)
		assert.Equal(t, key, tc.key, "key")
		assert.Equal(t, suffix, tc.suffix, "suffix")
	}
}

func TestReadWrite(t *testing.T) {
	dir, _ := ioutil.TempDir("", "test")
	defer os.RemoveAll(dir)
	tarfile := path.Join(dir, "temp.tar")
	inch := make(RawPipe, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, _ := GCreate(tarfile)
		defer stream.Close()
		TarRawSink(stream)(inch)
	}()
	inch <- Raw{"a", Bytes("A")}
	inch <- Raw{"b", Bytes("B")}
	inch <- Raw{"c", Bytes("C")}
	close(inch)
	wg.Wait()
	cmd := exec.Command("tar", "tvf", tarfile)
	rawoutput, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	output := string(rawoutput)
	assert.Contains(t, output, "bigdata/bigdata", "tar output")
	assert.Contains(t, output, " b", "tar output")
	outch := make(RawPipe, 100)
	go func() {
		stream, _ := GOpen(tarfile)
		defer stream.Close()
		TarRawSource(stream)(outch)
	}()
	sample := <-outch
	assert.Equal(t, sample.Key, "a", "record 1")
	assert.Equal(t, string(sample.Value), "A", "record 1")
	sample = <-outch
	assert.Equal(t, sample.Key, "b", "record 2")
	assert.Equal(t, string(sample.Value), "B", "record 2")
	sample = <-outch
	assert.Equal(t, sample.Key, "c", "record 3")
	assert.Equal(t, string(sample.Value), "C", "record 3")
	_, ok := (<-outch)
	if ok {
		t.Error("failed to close channel")
	}
}

func TestAggregate(t *testing.T) {
	inch := make(RawPipe, 100)
	outch := make(Pipe, 100)
	go Aggregate(inch, outch)
	inch <- Raw{"q/x.a", Bytes("A")}
	inch <- Raw{"q/x.b", Bytes("B")}
	inch <- Raw{"q/y.a", Bytes("A")}
	inch <- Raw{"q/y.b", Bytes("B")}
	inch <- Raw{"hello.x", Bytes("world")}
	close(inch)
	sample := <-outch
	assert.Equal(t, string(sample["__key__"]), "q/x", "sample")
	assert.Equal(t, string(sample["a"]), "A", "sample")
	assert.Equal(t, string(sample["b"]), "B", "sample")
	sample = <-outch
	assert.Equal(t, string(sample["__key__"]), "q/y", "sample")
	assert.Equal(t, string(sample["a"]), "A", "sample")
	assert.Equal(t, string(sample["b"]), "B", "sample")
	sample = <-outch
	assert.Equal(t, string(sample["__key__"]), "hello", "sample")
	assert.Equal(t, string(sample["x"]), "world", "sample")
}

func TestDisaggregate(t *testing.T) {
	inch := make(Pipe, 100)
	outch := make(RawPipe, 100)
	go Disaggregate(inch, outch)
	inch <- Sample{
		"__key__": Bytes("a/b"),
		"c.d":     Bytes("hello world"),
	}
	close(inch)
	sample := <-outch
	assert.Equal(t, "a/b.c.d", sample.Key, "key")
	assert.Equal(t, Bytes("hello world"), sample.Value, "value")
}

func TestRawSharding(t *testing.T) {
	dir, _ := ioutil.TempDir("", "test")
	defer os.RemoveAll(dir)
	shardpattern := path.Join(dir, "shard-%06d.tar")
	lastshard := fmt.Sprintf(shardpattern, 9)
	outch := make(RawPipe, 100)
	done := WaitFor(func() {
		ShardingRawTarSink(100, 1000000000, shardpattern, nil)(outch)
	})
	for i := 0; i < 1000; i++ {
		outch <- Raw{fmt.Sprintf("%06d", i), Bytes(fmt.Sprintf("%d", i))}
	}
	close(outch)
	<-done
	cmd := exec.Command("tar", "tvf", lastshard)
	rawoutput, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	output := string(rawoutput)
	assert.Contains(t, output, "bigdata/bigdata", "tar output")
	assert.Contains(t, output, "000999", "tar output")
}
