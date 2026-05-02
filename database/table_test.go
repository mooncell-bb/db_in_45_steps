package database

import (
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableByPKey(t *testing.T) {
	db := DB{}
	db.KV.Options.Dirpath = "test_db"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{1, 2}}, // (src, dst)
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
	db.KV.Options.Dirpath = "test_db_upsert"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{1, 2}},
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
	db.KV.Options.Dirpath = "test_db_update_mode"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		Indices: [][]int{{1, 2}},
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

func TestIterByPKey(t *testing.T) {
	db := DB{}
	db.KV.Options.Dirpath = "test_db_iter"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "k", Type: TypeI64},
			{Name: "v", Type: TypeI64},
		},
		Indices: [][]int{{0}},
	}

	N := int64(10)
	sorted := []int64{}
	for i := int64(0); i < N; i += 2 {
		sorted = append(sorted, i)
		row := Row{
			Cell{Type: TypeI64, I64: i},
			Cell{Type: TypeI64, I64: i},
		}
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	for i := int64(-1); i < N+1; i++ {
		row := Row{
			Cell{Type: TypeI64, I64: i},
			Cell{},
		}

		out := []int64{}
		iter, err := db.Seek(schema, row)
		for ; err == nil && iter.Valid(); err = iter.Next() {
			out = append(out, iter.Row()[1].I64)
		}
		require.Nil(t, err)

		expected := []int64{}
		for j := i; j < N; j++ {
			if j >= 0 && j%2 == 0 {
				expected = append(expected, j)
			}
		}
		assert.Equal(t, expected, out)
	}

	drainIter := func(req *RangeReq) (out []int64) {
		iter, err := db.Range(schema, req)
		for ; err == nil && iter.Valid(); err = iter.Next() {
			out = append(out, iter.Row()[1].I64)
		}
		require.Nil(t, err)
		return
	}
	testReq := func(req *RangeReq, i int64, j int64, desc bool) {
		out := drainIter(req)
		expected := rangeQuery(sorted, i, j, desc)
		require.Equal(t, expected, out)
	}

	for i := int64(-1); i < N+1; i++ {
		for j := int64(-1); j < N+1; j++ {
			req := &RangeReq{
				StartCmp: OP_GE,
				StopCmp:  OP_LE,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i, j, false)

			req = &RangeReq{
				StartCmp: OP_LE,
				StopCmp:  OP_GE,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i, j, true)

			req = &RangeReq{
				StartCmp: OP_GT,
				StopCmp:  OP_LT,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i+1, j-1, false)

			req = &RangeReq{
				StartCmp: OP_LT,
				StopCmp:  OP_GT,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i-1, j+1, true)
		}
	}

	for i := int64(-1); i < N+1; i++ {
		req := &RangeReq{
			StartCmp: OP_GE,
			StopCmp:  OP_LE,
			Start:    []Cell{{Type: TypeI64, I64: i}},
			Stop:     nil,
		}
		testReq(req, i, N, false)

		req = &RangeReq{
			StartCmp: OP_LE,
			StopCmp:  OP_GE,
			Start:    []Cell{{Type: TypeI64, I64: i}},
			Stop:     nil,
		}
		testReq(req, i, -1, true)
	}
}

func rangeQuery(sorted []int64, start int64, stop int64, desc bool) (out []int64) {
	for _, v := range sorted {
		if !desc && start <= v && v <= stop {
			out = append(out, v)
		} else if desc && stop <= v && v <= start {
			out = append(out, v)
		}
	}
	if desc {
		slices.Reverse(out)
	}
	return out
}

func TestIteratorValid(t *testing.T) {
	db := DB{}
	db.KV.Options.Dirpath = "test_db_valid"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "id", Type: TypeI64},
			{Name: "val", Type: TypeStr},
		},
		Indices: [][]int{{0}},
	}

	// Insert some test data
	rows := []Row{
		{Cell{Type: TypeI64, I64: 1}, Cell{Type: TypeStr, Str: []byte("one")}},
		{Cell{Type: TypeI64, I64: 3}, Cell{Type: TypeStr, Str: []byte("three")}},
		{Cell{Type: TypeI64, I64: 5}, Cell{Type: TypeStr, Str: []byte("five")}},
	}

	for _, row := range rows {
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	// Test Valid() returns true for valid iterator
	seekRow := Row{
		Cell{Type: TypeI64, I64: 1},
		Cell{},
	}
	iter, err := db.Seek(schema, seekRow)
	require.Nil(t, err)
	assert.True(t, iter.Valid())

	// Advance to next and verify still valid
	err = iter.Next()
	require.Nil(t, err)
	assert.True(t, iter.Valid())

	// Advance to next and verify still valid
	err = iter.Next()
	require.Nil(t, err)
	assert.True(t, iter.Valid())

	// Advance past end and verify not valid
	err = iter.Next()
	require.Nil(t, err)
	assert.False(t, iter.Valid())
}

func TestIteratorRow(t *testing.T) {
	db := DB{}
	db.KV.Options.Dirpath = "test_db_row"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "id", Type: TypeI64},
			{Name: "name", Type: TypeStr},
			{Name: "age", Type: TypeI64},
		},
		Indices: [][]int{{0}},
	}

	testData := []Row{
		{Cell{Type: TypeI64, I64: 100}, Cell{Type: TypeStr, Str: []byte("Alice")}, Cell{Type: TypeI64, I64: 25}},
		{Cell{Type: TypeI64, I64: 101}, Cell{Type: TypeStr, Str: []byte("Bob")}, Cell{Type: TypeI64, I64: 30}},
		{Cell{Type: TypeI64, I64: 102}, Cell{Type: TypeStr, Str: []byte("Charlie")}, Cell{Type: TypeI64, I64: 35}},
	}

	for _, row := range testData {
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	// Seek from the first row and verify Row() returns correct data
	seekRow := Row{
		Cell{Type: TypeI64, I64: 100},
		Cell{},
		Cell{},
	}
	iter, err := db.Seek(schema, seekRow)
	require.Nil(t, err)

	// Collect all rows via iterator
	collectedRows := []Row{}
	for iter.Valid() {
		collectedRows = append(collectedRows, append(Row{}, iter.Row()...))
		err = iter.Next()
		require.Nil(t, err)
	}

	assert.Equal(t, testData, collectedRows)
}

func TestIteratorNext(t *testing.T) {
	db := DB{}
	db.KV.Options.Dirpath = "test_db_next"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "id", Type: TypeI64},
			{Name: "val", Type: TypeI64},
		},
		Indices: [][]int{{0}},
	}

	// Insert test data: 0, 10, 20, 30, 40
	for i := int64(0); i < 5; i++ {
		row := Row{
			Cell{Type: TypeI64, I64: i * 10},
			Cell{Type: TypeI64, I64: i * 100},
		}
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	seekRow := Row{
		Cell{Type: TypeI64, I64: 0},
		Cell{},
	}
	iter, err := db.Seek(schema, seekRow)
	require.Nil(t, err)

	// Collect sequence of values using Next()
	values := []int64{}
	for iter.Valid() {
		values = append(values, iter.Row()[0].I64)
		err = iter.Next()
		require.Nil(t, err)
	}

	expected := []int64{0, 10, 20, 30, 40}
	assert.Equal(t, expected, values)
}
