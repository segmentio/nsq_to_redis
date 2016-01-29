package ratelimit

import (
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func TestExceeded(t *testing.T) {
	rl := New(1, 500)
	assert.Equal(t, rl.Exceeded("some-key"), false)
	assert.Equal(t, rl.Exceeded("some-key"), true)
	time.Sleep(time.Second) // :(
	assert.Equal(t, rl.Exceeded("some-key"), false)
	assert.Equal(t, rl.Exceeded("some-key"), true)
}

func TestMaxKeys(t *testing.T) {
	rl := New(1, 1)
	rl.Exceeded("a")
	rl.Exceeded("b")
	assert.Equal(t, rl.keys.Len(), 1)
}
