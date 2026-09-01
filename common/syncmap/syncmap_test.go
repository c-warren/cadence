package syncmap

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exclusively tests for races and crashes
func TestRace(t *testing.T) {
	t.Parallel()

	const parallel = 100
	var finish sync.WaitGroup
	var start sync.WaitGroup
	finish.Add(parallel)
	start.Add(parallel + 1)

	m := New[string, int]()
	for i := 0; i < parallel; i++ {
		i := i
		go func() {
			defer finish.Done()
			start.Done()
			start.Wait() // maximize contention
			key := strconv.Itoa(i % 3)
			if i%2 == 0 {
				_ = m.Put(key, i)
				_, _ = m.Get(key)
			} else {
				// reverse order to thrash a bit harder
				_, _ = m.Get(key)
				_ = m.Put(key, i)
			}
		}()
	}

	start.Done()
	finish.Wait()
}

// checks get/put behavior.  it's somewhat different and not *expected* to change,
// but should be totally fine to change to a general map API as long as existing uses are adapted.
func TestBasics(t *testing.T) {
	t.Parallel()

	const key = "key"
	const orig = 5
	const replacement = 10
	require.NotEqual(t, orig, replacement)

	m := New[string, int]()
	val, ok := m.Get(key)
	assert.False(t, ok, "should not get until a value is inserted")
	assert.Zero(t, val, "should get zero value if nothing inserted")

	inserted := m.Put(key, orig)
	assert.True(t, inserted, "should have inserted into empty map")

	val, ok = m.Get(key)
	assert.True(t, ok, "should successfully get after insert")
	assert.Equal(t, orig, val, "should get what was put")

	replaced := m.Put(key, replacement)
	assert.False(t, replaced, "should not replace existing values")

	val, ok = m.Get(key)
	assert.True(t, ok, "should still get")
	assert.Equal(t, orig, val, "should get original value, not replaced")
}
