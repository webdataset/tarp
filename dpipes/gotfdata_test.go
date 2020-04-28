package dpipes

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/stretchr/testify/assert"
)

type (
	SamplesReader struct {
		pipe Pipe
	}
)

func (r *SamplesReader) Read() (sample core.Sample, err error) {
	s, ok := <-r.pipe
	if !ok {
		return nil, io.EOF
	}

	return tarpSampleToTfDataSample(s), nil
}

func TFRecordSink(t *testing.T, writer io.Writer) Sink {
	return func(pipe Pipe) {
		w := core.NewTFRecordWriter(writer)
		samplesReader := &SamplesReader{pipe}
		tfExamplesReader := transform.SamplesToTFExample(samplesReader)
		err := w.WriteMessages(tfExamplesReader)

		assert.NoError(t, err)
	}
}

func TFRecordSource(t *testing.T, reader io.Reader) Source {
	return func(pipe Pipe) {
		defer close(pipe)
		var (
			ex *core.TFExample
			err error
			r core.TFExampleReader
		)
		r = core.NewTFRecordReader(reader)
		for ex, err = r.Read(); err == nil; ex, err = r.Read() {
			pipe <- tfExampleTarpSample(ex)
		}
		if err != io.EOF {
			assert.Fail(t, "expected to get io.EOF, got %v instead", err)
		}
	}
}

func SamplesChecker(t *testing.T, target int) Process {
	return func(in, out Pipe) {
		total := 0
		for s := range in {
			assert.Equal(t, s["txt"], Bytes(fmt.Sprintf("%d", total)))
			assert.Equal(t, s["__key__"], Bytes(fmt.Sprintf("%06d", total)))
			total++
			out <- s
		}
		close(out)
		assert.Equal(t, target, total)
	}
}

func tarpSampleToTfDataSample(sample Sample) core.Sample {
	s := core.NewSample()
	for k, v := range sample {
		s[k] = v
	}
	return s
}

func tfExampleTarpSample(example *core.TFExample) Sample {
	s := make(map[string]Bytes, len(example.GetFeatures().Feature))
	for k, v := range example.GetFeatures().Feature {
		var b Bytes
		err := json.Unmarshal(v.GetBytesList().Value[0], &b)
		if err != nil {
			panic(err)
		}
		s[k] = b // assume that all TFExample features are just a list of bytes
	}
	return s
}

func PrepareTarSource() Source {
	return func(pipe Pipe) {
		for i := 0; i < 1; i++ {
			pipe <- Sample{
				"__key__": Bytes(fmt.Sprintf("%06d", i)),
				"txt":     Bytes(fmt.Sprintf("%d", i)),
			}
		}
		close(pipe)
	}
}

func prepareTar(t *testing.T) *os.File {
	var (
		sinkFd   *os.File
		err      error
	)
	sinkFd, err = ioutil.TempFile("", "go-tfdata-*.tar")
	assert.NoError(t, err)

	sink := TarSink(sinkFd)
	Processing(PrepareTarSource(), nil, sink)
	return sinkFd
}

func TestGoTfData(t *testing.T) {
	var (
		sourceFd = prepareTar(t)
		sinkFd *os.File
		err error
	)

	defer os.RemoveAll(sourceFd.Name())
	sourceFd, err = os.Open(sourceFd.Name())
	assert.NoError(t, err)

	sinkFd, err = ioutil.TempFile("", "go-tfdata-*.tfrecord")
	assert.NoError(t, err)
	defer os.RemoveAll(sinkFd.Name())

	Processing(TarSource(sourceFd), nil, TFRecordSink(t, sinkFd))
	sinkFd.Close()
	sourceFd, err = os.Open(sinkFd.Name())
	assert.NoError(t, err)
	sinkFd, err = os.OpenFile(os.DevNull, os.O_RDWR, os.ModeAppend)
	assert.NoError(t, err)

	Processing(TFRecordSource(t, sourceFd), SamplesChecker(t, 1), TFRecordSink(t, sinkFd))
}
