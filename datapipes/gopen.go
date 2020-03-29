package datapipes

import (
	"io"
	"os"
	"os/exec"
	"strings"
)

// GOpen is a generic open that understands "-" and "pipe:" syntax.
func GOpen(fname string) (io.ReadCloser, error) {
	if fname == "-" {
		return os.Stdin, nil
	}
	if strings.HasPrefix(fname, "pipe:") {
		cmd := exec.Command("/bin/bash", "-c", fname[5:])
		stream, err := cmd.StdoutPipe()
		return stream, err
	}
	if strings.HasPrefix(fname, "file:") {
		fname = fname[5:]
	}
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
