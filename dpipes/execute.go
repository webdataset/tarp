package dpipes

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
)

// WriteBinary writes the bytes to disk at fname.
func WriteBinary(fname string, data []byte) {
	stream, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer stream.Close()
	_, err = stream.Write(data)
	if err != nil {
		panic(err)
	}
}

// ReadBinary reads an entire file and returns a byte array.
func ReadBinary(fname string) []byte {
	stream, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer stream.Close()
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	_, err = io.Copy(buffer, stream)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

// UnpackInDir unpacks a sample into a directory/prefix
// given by fprefix. You need to append the "." in fprefix
// since this function just concatenates fprefix with each
// key.
func UnpackInDir(sample Sample, dir, fprefix string) {
	prefix := path.Join(dir, fprefix)
	key := sample["__key__"]
	WriteBinary(prefix+"__key__", Bytes(key))
	for key, value := range sample {
		WriteBinary(prefix+key, value)
	}
}

// PackDir reads all the files under fprefix and puts
// them back into a sample.
func PackDir(dir, fprefix string) Sample {
	prefix := path.Join(dir, fprefix)
	matches, err := filepath.Glob(prefix + "*")
	Handle(err)
	var sample Sample = make(Sample)
	for _, match := range matches {
		data := ReadBinary(match)
		_, suffix := FnameSplit(match)
		sample[suffix] = data
	}
	return sample
}

// ExecuteOn unpacks data into a directory, executes the cmd
// in that directory, and then gathers up the result again.
func ExecuteOn(cmd, fprefix string) SampleF {
	return func(sample Sample) (Sample, error) {
		tmpdir, err := ioutil.TempDir(".", "*-execute")
		Handle(err)
		defer os.RemoveAll(tmpdir)
		UnpackInDir(sample, tmpdir, "sample.")
		matches, _ := filepath.Glob(tmpdir + "/*")
		Debug.Println("# ExecuteOn >", matches)
		fullcmd := "cd '" + tmpdir + "'; " + cmd
		proc := exec.Command("/bin/bash", "-c", fullcmd)
		data, err := proc.Output()
		matches, _ = filepath.Glob(tmpdir + "/*")
		Debug.Println("# ExecuteOn <", matches)
		Debug.Println("# ExecuteOn output", string(data))
		Debug.Println("# ExecuteOn err", err)
		osample := PackDir(tmpdir, "sample.")
		osample["__log__"] = data
		if err != nil {
			osample["__error__"] = Bytes(err.Error())
		}
		return osample, nil
	}
}

// ProcessSamples executes scripts on the files in a sampmle.
func ProcessSamples(cmd, fprefix string, ignoreerrs bool) Process {
	return MapSamples(ExecuteOn(cmd, fprefix), ignoreerrs)
}
