package dpipes

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSharding(t *testing.T) {
	dir, _ := ioutil.TempDir("", "test")
	defer os.RemoveAll(dir)
	shardpattern := path.Join(dir, "shard-%06d.tar")
	lastshard := fmt.Sprintf(shardpattern, 9)
	outch := make(Pipe, 100)
	done := WaitFor(func() {
		ShardingTarSink(100, 1000000000, shardpattern, nil)(outch)
	})
	for i := 0; i < 1000; i++ {
		outch <- Sample{
			"__key__": Bytes(fmt.Sprintf("%06d", i)),
			"txt":     Bytes(fmt.Sprintf("%d", i)),
		}
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
	assert.Contains(t, output, "000999.txt", "tar output")
}

func TestSources(t *testing.T) {
	dir, _ := ioutil.TempDir("", "test")
	defer os.RemoveAll(dir)
	tarfile := path.Join(dir, "sources.tar")
	stream, _ := GCreate(tarfile)
	outch := make(Pipe, 100)
	done := WaitFor(func() {
		go TarSink(stream)(outch)
	})
	for i := 0; i < 1000; i++ {
		outch <- Sample{
			"__key__": Bytes(fmt.Sprintf("%06d", i)),
			"txt":     Bytes(fmt.Sprintf("%d", i)),
		}
	}
	close(outch)
	<-done

	if true {
		Debug.Println("TestSources 1x")
		outch := make(Pipe, 100)
		go TarSources([]string{tarfile}, nil)(outch)
		count := CountSamples(outch)
		assert.Equal(t, 1000, count, "single tarsource")
		Debug.Println("TestSources 1x done")
	}

	if true {
		Debug.Println("TestSources 2x")
		outch := make(Pipe, 100)
		go TarSources([]string{tarfile, tarfile}, nil)(outch)
		count := CountSamples(outch)
		assert.Equal(t, 2000, count, "single tarsource")
		Debug.Println("TestSources 2x done")
	}

	if true {
		Debug.Println("TarMixer 2x")
		outch := make(Pipe, 100)
		go TarMixer([]string{tarfile, tarfile}, 2, 100, nil)(outch)
		count := CountSamples(outch)
		assert.Equal(t, 2000, count, "single tarsource")
		Debug.Println("TestSources 2x done")
	}
}
