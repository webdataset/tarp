package dpipes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShuffle(t *testing.T) {
	inch := make(Pipe, 100)
	outch := make(Pipe, 100)
	go func() {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("%06d", i)
			inch <- Sample{
				"__key__": Bytes(key),
				"txt":     Bytes(fmt.Sprintf("%d", i)),
			}
		}
		close(inch)
	}()
	go Shuffle(100, 10)(inch, outch)
	count := 0
	keycount := map[string]int{}
	for sample := range outch {
		count++
		keycount[string(sample["__key__"])]++
	}
	assert.Equal(t, 1000, count, "shuffle lost/gained element")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%06d", i)
		assert.Equal(t, 1, keycount[key], "lost/gained elements")
	}
}
