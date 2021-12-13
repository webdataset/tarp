package dpipes

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shamaton/msgpack"
	"gopkg.in/zeromq/goczmq.v4"
)

// MPSource reads concatenated msgpack from a stream and returns the samples.
// (Unimplemented.)
func MPSource(stream io.ReadCloser) func(Pipe) {
	panic("shamaton/msgpack does not support concatenated msgpack decoding")
}

// MPSink write concatenated msgpack to a stream.
func MPSink(stream io.WriteCloser) func(Pipe) {
	return func(inch Pipe) {
		for sample := range inch {
			data, err := msgpack.Encode(sample)
			Handle(err)
			stream.Write(data)
		}
		stream.Close()
	}
}

// MPTarSource reads a tar file and outputs a stream of samples.
func MPTarSource(stream io.ReadCloser) func(Pipe) {
	return func(outch Pipe) {
		rawinch := make(RawPipe, Pipesize)
		go TarRawSource(stream)(rawinch)
		for raw := range rawinch {
			sample := Sample{}
			msgpack.Decode(raw.Value, &sample)
			sample["__key__"] = Bytes(raw.Key)
			outch <- sample
		}
		close(outch)
		stream.Close()
	}
}

// MPTarSink writes a stream of samples to disk as a tar file.
func MPTarSink(stream io.WriteCloser) func(Pipe) {
	return func(inch Pipe) {
		rawoutch := make(RawPipe, Pipesize)
		go TarRawSink(stream)(rawoutch)
		for sample := range inch {
			data, err := msgpack.Encode(sample)
			Handle(err)
			key := string(sample["__key__"])
			rawoutch <- Raw{key + ".mp", data}
		}
		close(rawoutch)
	}
}

// Definitions for our URL schemes for opening sockets.
// General syntax is:
// zmq_socket_type[+transport]://host:port
var zschemes = []struct {
	name string
	kind int
	bind bool
}{
	{"zpush", goczmq.Push, false},
	{"zpull", goczmq.Pull, true},
	{"zpub", goczmq.Pub, true},
	{"zsub", goczmq.Sub, false},
	{"zrpush", goczmq.Push, true},
	{"zrpull", goczmq.Pull, false},
	{"zrpub", goczmq.Pub, false},
	{"zrsub", goczmq.Sub, true},
}

// ZMQOpen takes a zmq URL spec and returns a socket.
func ZMQOpen(rawurl string) (*goczmq.Sock, error) {
	fields, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if fields.Scheme == "" {
		return nil, errors.New("no scheme given")
	}
	schemes := strings.Split(fields.Scheme+"+tcp", "+")
	kind := -1
	bind := false
	for _, r := range zschemes {
		if r.name == schemes[0] {
			kind = r.kind
			bind = r.bind
		}
	}
	Assert(kind != -1, fields.Scheme+": no scheme found")
	sock := goczmq.NewSock(kind)
	where := fmt.Sprintf("%s://%s", schemes[1], fields.Host)
	if bind {
		sock.Bind(where)
	} else {
		sock.Connect(where)
	}
	return sock, nil
}

// ZMQSource reads samples from a ZMQ socket and
// adds them to the Pipe. Encoding is in msgpack format.
func ZMQSource(rawurl string, eof bool) func(Pipe) {
	return func(outch Pipe) {
		sock, err := ZMQOpen(rawurl)
		Handle(err)
		for {
			data, more, err := sock.RecvFrame()
			Handle(err)
			Assert(more == 0, "unexpected multipart")
			sample := Sample{}
			err = msgpack.Decode(data, &sample)
			Handle(err)
			Assert(len(sample) > 0, "samples must be non-empty")
			Debug.Println("ZMQSource\n" + StrSample(sample))
			_, ok := sample["__EOF__"]
			if ok {
				Debug.Println("received EOF on ZMQ")
				if eof {
					break
				} else {
					continue
				}
			}
			outch <- sample
		}
		close(outch)
	}
}

// ZMQSink takes samples from the Pipe and
// sends them to the ZMQ socket. Encoding is in
// msgpack format.
func ZMQSink(rawurl string, eof bool) func(Pipe) {
	return func(inch Pipe) {
		Debug.Println("ZMQSink start")
		sock, err := ZMQOpen(rawurl)
		Handle(err)
		for sample := range inch {
			Assert(len(sample) > 0, "samples must be non-empty")
			data, err := msgpack.Encode(sample)
			Handle(err)
			sock.SendFrame(data, 0)
			Debug.Println("ZMQSink sent\n" + StrSample(sample))
		}
		if eof {
			sample := Sample{"__EOF__": Bytes("true")}
			data, _ := msgpack.Encode(sample)
			sock.SendFrame(data, 0)
			Debug.Println("sent EOF on ZMQ")
		}
		Debug.Println("ZMQSink done")
		// FIXME workaround for bug in ZMQ send
		delay, err := strconv.ParseFloat(GetEnv("ZMQ_CLOSE_DELAY", "5.0"), 64)
		Handle(err)
		time.Sleep(time.Duration(delay * float64(time.Second)))
	}
}
