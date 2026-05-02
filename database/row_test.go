package database

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowEncode(t *testing.T) {
	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{2, 1}}, // (dst, src)
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	key := []byte{'l', 'i', 'n', 'k', 0, byte(TypeStr), 'b', 0, byte(TypeStr), 'a', 0, 0}
	val := []byte{123, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, key, row.EncodeKey(schema))
	assert.Equal(t, val, row.EncodeVal(schema))

	decoded := schema.NewRow()
	err := decoded.DecodeKey(schema, key)
	assert.Nil(t, err)
	err = decoded.DecodeVal(schema, val)
	assert.Nil(t, err)
	assert.Equal(t, row, decoded)

	rows := []Row{
		{
			Cell{Type: TypeI64, I64: 123},
			Cell{Type: TypeStr, Str: []byte("ba")},
			Cell{Type: TypeStr, Str: []byte("b")},
		},
		{
			Cell{Type: TypeI64, I64: 123},
			Cell{Type: TypeStr, Str: []byte("a")},
			Cell{Type: TypeStr, Str: []byte("bb")},
		},
		{
			Cell{Type: TypeI64, I64: 123},
			Cell{Type: TypeStr, Str: []byte("a")},
			Cell{Type: TypeStr, Str: []byte("bba")},
		},
	}
	keys := []string{}
	for _, row = range rows {
		key = row.EncodeKey(schema)
		keys = append(keys, string(key))

		decoded = schema.NewRow()
		err = decoded.DecodeKey(schema, key)
		assert.Nil(t, err)
		err = decoded.DecodeVal(schema, val)
		assert.Nil(t, err)
		assert.Equal(t, row, decoded)
	}
	assert.True(t, slices.IsSorted(keys))
}

func TestRowEncodePanicsOnSchemaMismatch(t *testing.T) {
	schema := &Schema{
		Table:   "t1",
		Cols:    []Column{{Name: "c1", Type: TypeI64}},
		Indices: [][]int{{0}},
	}

	row := Row{}
	assert.Panics(t, func() { _ = row.EncodeKey(schema) })
	assert.Panics(t, func() { _ = row.EncodeVal(schema) })
}

func TestRowEncodePanicsOnTypeMismatch(t *testing.T) {
	schema := &Schema{
		Table:   "t1",
		Cols:    []Column{{Name: "c1", Type: TypeI64}},
		Indices: [][]int{{0}},
	}

	row := Row{Cell{Type: TypeStr, Str: []byte("x")}}
	assert.Panics(t, func() { _ = row.EncodeKey(schema) })
	assert.Panics(t, func() { _ = row.EncodeVal(schema) })
}

func TestRowDecodeKeyErrors(t *testing.T) {
	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{1, 2}},
	}

	row := schema.NewRow()

	_, err := func() (bool, error) { return false, row.DecodeKey(schema, []byte("wrong")) }()
	assert.Error(t, err)

	// trailing garbage
	badKey := append([]byte("link\x00"), []byte{1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 0xff}...)
	err = row.DecodeKey(schema, badKey)
	assert.Error(t, err)
}

func TestRowDecodeValErrors(t *testing.T) {
	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{1, 2}},
	}

	row := schema.NewRow()

	// not enough bytes for i64
	err := row.DecodeVal(schema, []byte{1, 0, 0})
	assert.Error(t, err)

	// trailing garbage after value
	val := []byte{123, 0, 0, 0, 0, 0, 0, 0}
	err = row.DecodeVal(schema, append(val, 0xff))
	assert.Error(t, err)
}
