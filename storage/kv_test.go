package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVBasic(t *testing.T) {
	kv := KV{}

	// Open
	err := kv.Open()
	assert.NoError(t, err)
	defer kv.Close()

	// Set new key
	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Get existing key
	val, ok, err := kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v1"), val)

	// Get non-existing key
	val, ok, err = kv.Get([]byte("xxx"))
	assert.NoError(t, err)
	assert.False(t, ok)

	// Delete non-existing key
	updated, err = kv.Del([]byte("xxx"))
	assert.NoError(t, err)
	assert.False(t, updated)

	// Delete existing key
	updated, err = kv.Del([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Verify key is deleted
	val, ok, err = kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestKVUpdateValue(t *testing.T) {
	kv := KV{}
	assert.NoError(t, kv.Open())
	defer kv.Close()

	// Set initial value
	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Update with different value
	updated, err = kv.Set([]byte("k1"), []byte("v2"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Verify updated value
	val, ok, err := kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v2"), val)
}

func TestKVSameValue(t *testing.T) {
	kv := KV{}
	assert.NoError(t, kv.Open())
	defer kv.Close()

	// Set value
	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Set same value again
	updated, err = kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	assert.False(t, updated)
}

func TestEntryEncode(t *testing.T) {
	ent := Entry{key: []byte("k1"), val: []byte("xxx")}
	data := []byte{2, 0, 0, 0, 3, 0, 0, 0, 'k', '1', 'x', 'x', 'x'}

	// Encode
	assert.Equal(t, data, ent.Encode())

	// Decode
	decoded := Entry{}
	err := decoded.Decode(bytes.NewBuffer(data))
	assert.Nil(t, err)
	assert.Equal(t, ent, decoded)
}
