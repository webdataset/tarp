package dpipes

import (
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
	io.Reader
	cmd *exec.Cmd
}

func (c waitCloser) Close() error {
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
