package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKVBasic(t *testing.T) {
	kv := KV{}
	kv.Log.FileName = ".test_kv_basic_db"

	defer os.Remove(".test_kv_basic_db")
	os.Remove(".test_kv_basic_db")

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
	_, ok, err = kv.Get([]byte("xxx"))
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
	_, ok, err = kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.False(t, ok)

	// Set another key for persistence check
	updated, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.NoError(t, err)
	assert.True(t, updated)

	// Reopen
	assert.NoError(t, kv.Close())

	err = kv.Open()
	assert.NoError(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.False(t, ok)

	val, ok, err = kv.Get([]byte("k2"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v2"), val)
}

func TestKVUpdateValue(t *testing.T) {
	kv := KV{}
	kv.Log.FileName = ".test_kv_update_db"
	defer os.Remove(kv.Log.FileName)
	_ = os.Remove(kv.Log.FileName)

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
	kv.Log.FileName = ".test_kv_same_db"
	defer os.Remove(kv.Log.FileName)
	_ = os.Remove(kv.Log.FileName)

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
	data := []byte{0xe9, 0xec, 0x4d, 0x9e, 2, 0, 0, 0, 3, 0, 0, 0, 0, 'k', '1', 'x', 'x', 'x'}

	// Encode
	assert.Equal(t, data, ent.Encode())

	// Decode
	decoded := Entry{}
	err := decoded.Decode(bytes.NewBuffer(data))
	assert.NoError(t, err)
	assert.Equal(t, ent, decoded)

	ent = Entry{key: []byte("k1"), deleted: true}
	data = []byte{0x4c, 0xd0, 0xfe, 0xe5, 2, 0, 0, 0, 0, 0, 0, 0, 1, 'k', '1'}

	// Encode deleted entry
	assert.Equal(t, data, ent.Encode())

	// Decode deleted entry
	decoded = Entry{}
	err = decoded.Decode(bytes.NewBuffer(data))
	assert.NoError(t, err)
	assert.Equal(t, ent, decoded)
}

func TestLogWriteRead(t *testing.T) {
	Log := Log{FileName: ".test_log_rw"}
	defer os.Remove(Log.FileName)
	_ = os.Remove(Log.FileName)

	// Open and write entries
	assert.NoError(t, Log.Open())
	assert.NoError(t, Log.Write(&Entry{key: []byte("k1"), val: []byte("v1")}))
	assert.NoError(t, Log.Write(&Entry{key: []byte("k1"), deleted: true}))
	assert.NoError(t, Log.Close())

	// Reopen and read first entry
	assert.NoError(t, Log.Open())
	defer Log.Close()

	ent := Entry{}
	eof, err := Log.Read(&ent)
	assert.NoError(t, err)
	assert.False(t, eof)
	assert.Equal(t, Entry{key: []byte("k1"), val: []byte("v1")}, ent)

	// Read second entry
	ent = Entry{}
	eof, err = Log.Read(&ent)
	assert.NoError(t, err)
	assert.False(t, eof)
	assert.Equal(t, Entry{key: []byte("k1"), deleted: true}, ent)

	// Read EOF
	ent = Entry{}
	eof, err = Log.Read(&ent)
	assert.NoError(t, err)
	assert.True(t, eof)
}

func TestLogReadEOF(t *testing.T) {
	Log := Log{FileName: ".test_log_eof"}
	defer os.Remove(Log.FileName)
	_ = os.Remove(Log.FileName)

	assert.NoError(t, Log.Open())
	defer Log.Close()

	// Empty Log should return EOF directly
	ent := Entry{}
	eof, err := Log.Read(&ent)
	assert.NoError(t, err)
	assert.True(t, eof)
}

func TestKVUpdateMode(t *testing.T) {
	kv := KV{}
	kv.Log.FileName = ".test_db_update_mode"
	defer os.Remove(kv.Log.FileName)

	os.Remove(kv.Log.FileName)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	updated, err := kv.SetEx([]byte("k1"), []byte("v1"), ModeUpdate)
	assert.True(t, !updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v1"), ModeUpdate)
	assert.True(t, !updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v1"), ModeInsert)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("xx"), ModeInsert)
	assert.True(t, !updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("yy"), ModeUpdate)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("zz"), ModeUpsert)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k2"), []byte("tt"), ModeUpsert)
	assert.True(t, updated && err == nil)
}

func TestKVSeek(t *testing.T) {
	kv := KV{}
	kv.Log.FileName = ".test_db"
	defer os.Remove(kv.Log.FileName)

	os.Remove(kv.Log.FileName)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	keys := []string{"c", "e", "g"}
	vals := []string{"3", "5", "7"}
	for i := range keys {
		_, _ = kv.Set([]byte(keys[i]), []byte(vals[i]))
	}

	iter, err := kv.Seek([]byte("a"))
	require.Nil(t, err)
	for i := range keys {
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte(keys[i]), iter.Key())
		assert.Equal(t, []byte(vals[i]), iter.Val())
		err = iter.Next()
		require.Nil(t, err)
	}
	assert.False(t, iter.Valid())

	err = iter.Prev()
	require.Nil(t, err)
	for i := len(keys) - 1; i >= 0; i-- {
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte(keys[i]), iter.Key())
		assert.Equal(t, []byte(vals[i]), iter.Val())
		err = iter.Prev()
		require.Nil(t, err)
	}
	assert.False(t, iter.Valid())

	iter, err = kv.Seek([]byte("f"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("g"), iter.Key())

	iter, err = kv.Seek([]byte("g"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("g"), iter.Key())

	iter, err = kv.Seek([]byte("h"))
	require.Nil(t, err)
	assert.False(t, iter.Valid())
}
