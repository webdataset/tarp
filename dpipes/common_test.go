package dpipes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpandBraces(t *testing.T) {
	result := ExpandBraces("{0..1}-{0..9}")
	assert.Equal(t, 20, len(result), "expand len")
	assert.Equal(t, result[19], "1-9", "result value")
}
