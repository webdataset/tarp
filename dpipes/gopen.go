package dpipes

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

// TODO
// - make this table/registry driven
// - refactor into separate library
// - support common protocols directly via Go libraries

type waitCloser struct {
	io.ReadCloser
	cmd *exec.Cmd
}

func (c waitCloser) Close() error {
	c.ReadCloser.Close()
	return c.cmd.Wait()
}

// GOpen is a generic open that understands "-" and "pipe:" syntax.
func GOpen(fname string) (io.ReadCloser, error) {
	if fname == "-" {
		return os.Stdin, nil
	}
	if strings.HasPrefix(fname, "text:") {
		data := fname[5:]
		Debug.Println("text", data)
		return ioutil.NopCloser(strings.NewReader(data)), nil
	}
	if strings.HasPrefix(fname, "pipe:") {
		cmd := exec.Command("/bin/bash", "-c", fname[5:])
		Debug.Println("exec.Command", cmd)
		stream, err := cmd.StdoutPipe()
		Handle(err)
		stream2 := waitCloser{stream, cmd}
		cmd.Start()
		return stream2, err
	}
	if strings.HasPrefix(fname, "file:") {
		fname = fname[5:]
	}
	Debug.Println("open", fname)
	return os.Open(fname)
}

// GCreate is a generic create that understands "-" and "pipe:" syntax.
func GCreate(fname string) (io.WriteCloser, error) {
	if fname == "-" {
		return os.Stdout, nil
	}
	if strings.HasPrefix(fname, "pipe:") {
		cmd := exec.Command("/bin/bash", "-c", fname[5:])
		stream, err := cmd.StdinPipe()
		return stream, err
	}
	if strings.HasPrefix(fname, "file:") {
		fname = fname[5:]
	}
	return os.Create(fname)
}

// WriteBinary writes the bytes to disk at fname.
func WriteBinary(fname string, data []byte) error {
	stream, err := GCreate(fname)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.Write(data)
	return err
}

// ReadBinary reads an entire file and returns a byte array.
func ReadBinary(fname string) ([]byte, error) {
	stream, err := GOpen(fname)
	if err != nil {
		return make([]byte, 0), err
	}
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	_, err = io.Copy(buffer, stream)
	if err != nil {
		return buffer.Bytes(), err
	}
	err = stream.Close()
	return buffer.Bytes(), err
}
