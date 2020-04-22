package dpipes

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
)

// WriteBinary writes the bytes to disk at fname.
func WriteBinary(fname string, data []byte) {
	stream, err := GCreate(fname)
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
	stream, err := GOpen(fname)
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
func ExecuteOn(cmd string) SampleF {
	abort := (GetEnv("ExecuteOnAbort", "yes") == "no")
	return func(sample Sample) (Sample, error) {
		tmpdir, err := ioutil.TempDir(".", "*-execute")
		Handle(err)
		defer os.RemoveAll(tmpdir)
		UnpackInDir(sample, tmpdir, "sample.")
		matches, _ := filepath.Glob(tmpdir + "/sample.*")
		Debug.Println("# ExecuteOn >", matches)
		fullcmd := "cd '" + tmpdir + "' > /dev/null; " + cmd
		proc := exec.Command("/bin/bash", "-c", fullcmd)
		proc.Stdout = os.Stderr
		proc.Stderr = os.Stderr
		err = proc.Run()
		if err != nil {
			Progress.Println(err.Error())
			if abort {
				panic("error in ExecuteOn")
			}
		}
		matches, _ = filepath.Glob(tmpdir + "/sample.*")
		osample := PackDir(tmpdir, "sample.")
		return osample, nil
	}
}

// ProcessSamples executes scripts on the files in a sampmle.
func ProcessSamples(cmd string, ignoreerrs bool) Process {
	return MapSamples(ExecuteOn(cmd), ignoreerrs)
}

func isdir(s string) bool {
	mode, _ := os.Stat(s)
	return mode.IsDir()
}

var MultiFmt string = GetEnv("ExecuteOnFmt", "%06d")

// MultiExecuteOn unpacks data into a directory, executes the cmd
// in that directory, and then gathers up all subdirectories as samples
func MultiExecuteOn(cmd string) MultiSampleF {
	abort := (GetEnv("ExecuteOnAbort", "yes") == "no")
	return func(sample Sample) ([]Sample, error) {
		tmpdir, err := ioutil.TempDir(".", "*-execute")
		Handle(err)
		defer os.RemoveAll(tmpdir)
		UnpackInDir(sample, tmpdir, "sample.")
		matches, _ := filepath.Glob(tmpdir + "/*")
		Debug.Println("# ExecuteOn >", matches)
		fullcmd := "cd '" + tmpdir + "' > /dev/null; " + cmd
		proc := exec.Command("/bin/bash", "-c", fullcmd)
		proc.Stdout = os.Stderr
		proc.Stderr = os.Stderr
		err = proc.Run()
		if err != nil {
			Progress.Println(err.Error())
			if abort {
				panic("error in ExecuteOn")
			}
		}
		result := make([]Sample, 0, 100)
		for i := 0; i <= 1000000; i++ {
			prefix := fmt.Sprintf("%s-" + MultiFmt, "sample", i)
			fnames, err := filepath.Glob(tmpdir + "/" + prefix + ".*")
			if err != nil || len(fnames) < 1 {
				// allow gaps in the numbering for the first 100
				if i >= 100 {
					break
				} else {
					continue
				}
			}
			Debug.Println("# MultiExecuteOn <", fnames)
			osample := PackDir(tmpdir, prefix+".")
			if string(osample["__key__"]) == "" {
				if string(sample["__key__"]) != "" {
					osample["__key__"] = Bytes(string(sample["__key__"]) + "/" + fmt.Sprintf(MultiFmt, i))
				}
			}
			result = append(result, osample)
		}
		return result, nil
	}
}

func MultiProcessSamples(cmd string, ignoreerrs bool) Process {
	return MultiMapSamples(MultiExecuteOn(cmd), ignoreerrs)
}

