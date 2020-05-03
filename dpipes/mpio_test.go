// +build mpio

package dpipes

import (
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZMQOpen(t *testing.T) {
	sender, err := ZMQOpen("zpush://127.0.0.1:9122")
	if err != nil {
		panic(err)
	}
	defer sender.Destroy()
	receiver, err := ZMQOpen("zpull://127.0.0.1:9122")
	if err != nil {
		panic(err)
	}
	defer receiver.Destroy()
	original := Bytes("hello")
	err = sender.SendFrame(original, 0)
	if err != nil {
		panic(err)
	}
	data, more, err := receiver.RecvFrame()
	if more != 0 {
		panic("unexpected multipart")
	}
	if err != nil {
		panic(err)
	}
	assert.Equal(t, string(original), string(data), "zmq failed")
}

func TestTransmit(t *testing.T) {
	inch := make(Pipe, 100)
	outch := make(Pipe, 100)
	sample := Sample{}
	sample["a"] = Bytes("A")
	go ZMQSource("zpull://127.0.0.1:9123", true)(outch)
	go ZMQSink("zpush://127.0.0.1:9123", true)(inch)
	Debug.Println("sent\n" + StrSample(sample))
	inch <- sample
	result := <-outch
	Debug.Println("received\n" + StrSample(result))
	// assert.Greater(t, len(sample), 0, "transmit failed")
	// assert.Greater(t, len(result), 0, "transmit failed")
	assert.Equal(t, sample, result, "transmit failed")
}

func TestMPTar(t *testing.T) {
	// dir, _ := ioutil.TempDir("", "test")
	// defer os.RemoveAll(dir)
	dir := "./"
	tarfile := path.Join(dir, "mptemp.tar")
	inch := make(Pipe, 100)
	done := WaitFor(func() {
		stream, _ := GCreate(tarfile)
		MPTarSink(stream)(inch)
	})
	inch <- Sample{
		"__key__": Bytes("q"),
		"a":       Bytes("A"),
	}
	close(inch)
	<-done
	cmd := exec.Command("tar", "tvf", tarfile)
	rawoutput, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	output := string(rawoutput)
	assert.Contains(t, output, "bigdata/bigdata", "tar output")
	assert.Contains(t, output, "q.mp", "tar output")
	outch := make(Pipe, 100)
	done = WaitFor(func() {
		stream, _ := GOpen(tarfile)
		defer stream.Close()
		MPTarSource(stream)(outch)
	})
	sample := <-outch
	assert.Equal(t, string(sample["a"]), "A")
	_, ok := (<-outch)
	if ok {
		t.Error("failed to close channel")
	}
	<-done
}
