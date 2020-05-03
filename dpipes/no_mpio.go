// +build !mpio

package dpipes

import (
	"io"

	"gopkg.in/zeromq/goczmq.v4"
)

// MPSource reads concatenated msgpack from a stream and returns the samples.
// (Unimplemented.)
func MPSource(stream io.ReadCloser) func(Pipe) {
	panic("compiled without mpio")
}

// MPSink write concatenated msgpack to a stream.
func MPSink(stream io.WriteCloser) func(Pipe) {
	panic("compiled without mpio")
}

// MPTarSource reads a tar file and outputs a stream of samples.
func MPTarSource(stream io.ReadCloser) func(Pipe) {
	panic("compiled without mpio")
}

// MPTarSink writes a stream of samples to disk as a tar file.
func MPTarSink(stream io.WriteCloser) func(Pipe) {
	panic("compiled without mpio")
}

// ZMQOpen takes a zmq URL spec and returns a socket.
func ZMQOpen(rawurl string) (*goczmq.Sock, error) {
	panic("compiled without mpio")
}

// ZMQSource reads samples from a ZMQ socket and
// adds them to the Pipe. Encoding is in msgpack format.
func ZMQSource(rawurl string, eof bool) func(Pipe) {
	panic("compiled without mpio")
}

// ZMQSink takes samples from the Pipe and
// sends them to the ZMQ socket. Encoding is in
// msgpack format.
func ZMQSink(rawurl string, eof bool) func(Pipe) {
	panic("compiled without mpio")
}
