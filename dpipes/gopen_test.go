package dpipes

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGopenFile(t *testing.T) {
	dir, _ := ioutil.TempDir("", "test")
	defer os.RemoveAll(dir)

	fname := dir + "/temp"
	bytes := []byte("hello world")

	{
		stream, err := GCreate(fname)
		assert.Nil(t, err)
		stream.Write(bytes)
		err = stream.Close()
		assert.Nil(t, err)
	}

	{
		stream, err := GOpen(fname)
		assert.Nil(t, err)
		data := make([]byte, 10000)
		n, err := stream.Read(data)
		assert.Nil(t, err)
		assert.Equal(t, 11, n)
		data = data[:n]
		err = stream.Close()
		assert.Nil(t, err)
		assert.Equal(t, data, bytes)
	}
}

func TestGopenText(t *testing.T) {
	stream, err := GOpen("text:abcdef")
	assert.Nil(t, err)
	data := make([]byte, 10000)
	n, err := stream.Read(data)
	assert.Nil(t, err)
	assert.Equal(t, 6, n)
	err = stream.Close()
	assert.Nil(t, err)
	data = data[:n]
	assert.Equal(t, []byte("abcdef"), data)
}

func TestGopenPipe(t *testing.T) {
	stream, err := GOpen("pipe:echo abcdef")
	assert.Nil(t, err)
	data := make([]byte, 10000)
	n, err := stream.Read(data)
	assert.Nil(t, err)
	assert.Equal(t, 7, n)
	err = stream.Close()
	assert.Nil(t, err)
	data = data[:n]
	assert.Equal(t, data, []byte("abcdef\n"))
}
