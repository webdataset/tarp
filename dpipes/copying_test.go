package dpipes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSliceSpec(t *testing.T) {
	lo, hi, step := ParseSliceSpec("13:17:9")
	assert.Equal(t, 13, lo, "lo")
	assert.Equal(t, 17, hi, "hi")
	assert.Equal(t, 9, step, "step")
}

func TestParseSliceSpec2(t *testing.T) {
	lo, hi, step := ParseSliceSpec(":999")
	assert.Equal(t, 0, lo, "lo")
	assert.Equal(t, 999, hi, "hi")
	assert.Equal(t, 1, step, "step")
}

func TestParseSliceSpec3(t *testing.T) {
	lo, hi, step := ParseSliceSpec("3:19")
	assert.Equal(t, 3, lo, "lo")
	assert.Equal(t, 19, hi, "hi")
	assert.Equal(t, 1, step, "step")
}

func TestParseSliceSpec4(t *testing.T) {
	lo, hi, step := ParseSliceSpec("3:")
	assert.Equal(t, 3, lo, "lo")
	assert.Equal(t, 999999999, hi, "hi")
	assert.Equal(t, 1, step, "step")
}
