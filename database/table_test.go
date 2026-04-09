package database

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableByPKey(t *testing.T) {
	db := DB{}
	db.KV.Log.FileName = ".test_db"

	err := db.Open()
	defer os.Remove(".test_db")

	os.Remove(".test_db")
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		PKey: []int{1, 2}, // (src, dst)
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err := db.Select(schema, row)
	assert.True(t, !ok && err == nil)

	updated, err := db.Insert(schema, row)
	assert.True(t, updated && err == nil)

	out := Row{
		Cell{Type: TypeI64},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	row[0].I64 = 456
	updated, err = db.Update(schema, row)
	assert.True(t, updated && err == nil)

	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	deleted, err := db.Delete(schema, row)
	assert.True(t, deleted && err == nil)

	ok, err = db.Select(schema, row)
	assert.True(t, !ok && err == nil)
}

func TestTableUpsert(t *testing.T) {
	db := DB{}
	db.KV.Log.FileName = ".test_db_upsert"

	err := db.Open()
	defer os.Remove(".test_db_upsert")

	os.Remove(".test_db_upsert")
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		PKey: []int{1, 2},
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}

	// First time Upsert should act as Insert
	updated, err := db.Upsert(schema, row)
	assert.True(t, updated && err == nil)

	out := Row{
		Cell{Type: TypeI64},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err := db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	// Second time Upsert should act as Update
	row[0].I64 = 456
	updated, err = db.Upsert(schema, row)
	assert.True(t, updated && err == nil)

	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)
}

func TestTableUpdateMode(t *testing.T) {
	db := DB{}
	db.KV.Log.FileName = ".test_db_update_mode"

	err := db.Open()
	defer os.Remove(".test_db_update_mode")

	os.Remove(".test_db_update_mode")
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		PKey: []int{1, 2},
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}

	// Update on non-existent row should fail (not updated)
	updated, err := db.Update(schema, row)
	assert.True(t, !updated && err == nil)

	// Insert row
	updated, err = db.Insert(schema, row)
	assert.True(t, updated && err == nil)

	// Insert same row again should act as ModeInsert -> fail existing check
	updated, err = db.Insert(schema, row)
	assert.True(t, !updated && err == nil)

	// Update existing row should succeed
	row[0].I64 = 789
	updated, err = db.Update(schema, row)
	assert.True(t, updated && err == nil)

	// Upsert same unchanged value should return updated=false
	updated, err = db.Upsert(schema, row)
	assert.True(t, !updated && err == nil)
}
