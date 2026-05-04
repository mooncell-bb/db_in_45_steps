package parser

import (
	"os"
	"slices"
	"testing"

	"github.com/mooncell-bb/db_in_45_steps/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseStmt(t *testing.T, s string) any {
	p := NewParser(s)
	stmt, err := p.ParseStmt()
	require.Nil(t, err)
	return stmt
}

func TestSQLByPKey(t *testing.T) {
	db := &database.DB{}
	exec := &Exec{DB: db}
	db.KV.Options.Dirpath = "test_db_sql_by_pkey"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := "create table link (time int64, src string, dst string, primary key (src, dst));"
	_, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	s = "insert into link values (123, 'bob', 'alice');"
	r, err := exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{database.Cell{Type: database.TypeI64, I64: 123}}}, r.Values)

	s = "update link set time = 456 where dst = 'alice' and src = 'bob';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{database.Cell{Type: database.TypeI64, I64: 456}}}, r.Values)

	s = "insert into link values (123, 'cde', 'fgh');"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where src >= 'b';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(456)}, {makeCell(123)}}, r.Values)

	s = "select time from link where 'b' <= src;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(456)}, {makeCell(123)}}, r.Values)

	s = "select time from link where src <= 'z';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(123)}, {makeCell(456)}}, r.Values)

	s = "select time from link where 'cde' > src;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(456)}}, r.Values)

	s = "select time from link where (src, dst) >= ('bob', 'alice');"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(456)}, {makeCell(123)}}, r.Values)

	s = "select time from link where (src, dst) >= ('bob', 'alicf');"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{{makeCell(123)}}, r.Values)

	// reopen
	err = db.Close()
	require.Nil(t, err)
	db = &database.DB{}
	exec = &Exec{DB: db}
	db.KV.Options.Dirpath = "test_db_sql_by_pkey"
	err = db.Open()
	require.Nil(t, err)
	defer db.Close()

	s = "delete from link where src = 'bob' and dst = 'alice';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 0, len(r.Values))
}

func TestIterByPKey(t *testing.T) {
	db := &database.DB{}
	db.KV.Options.Dirpath = "test_db_iter_by_pkey"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &database.Schema{
		Table: "t",
		Cols: []database.Column{
			{Name: "k", Type: database.TypeI64},
			{Name: "v", Type: database.TypeI64},
		},
		Indices: [][]int{{0}},
	}

	N := int64(10)
	sorted := []int64{}
	for i := int64(0); i < N; i += 2 {
		sorted = append(sorted, i)
		row := database.Row{
			database.Cell{Type: database.TypeI64, I64: i},
			database.Cell{Type: database.TypeI64, I64: i},
		}
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	for i := int64(-1); i < N+1; i++ {
		row := database.Row{
			database.Cell{Type: database.TypeI64, I64: i},
			database.Cell{},
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

	drainIter := func(req *database.RangeReq) (out []int64) {
		iter, err := db.Range(schema, req)
		for ; err == nil && iter.Valid(); err = iter.Next() {
			out = append(out, iter.Row()[1].I64)
		}
		require.Nil(t, err)
		return
	}
	testReq := func(req *database.RangeReq, i int64, j int64, desc bool) {
		out := drainIter(req)
		expected := rangeQuery(sorted, i, j, desc)
		require.Equal(t, expected, out)
	}

	for i := int64(-1); i < N+1; i++ {
		for j := int64(-1); j < N+1; j++ {
			req := &database.RangeReq{
				StartCmp: database.OP_GE,
				StopCmp:  database.OP_LE,
				Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
				Stop:     []database.Cell{{Type: database.TypeI64, I64: j}},
			}
			testReq(req, i, j, false)

			req = &database.RangeReq{
				StartCmp: database.OP_LE,
				StopCmp:  database.OP_GE,
				Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
				Stop:     []database.Cell{{Type: database.TypeI64, I64: j}},
			}
			testReq(req, i, j, true)

			req = &database.RangeReq{
				StartCmp: database.OP_GT,
				StopCmp:  database.OP_LT,
				Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
				Stop:     []database.Cell{{Type: database.TypeI64, I64: j}},
			}
			testReq(req, i+1, j-1, false)

			req = &database.RangeReq{
				StartCmp: database.OP_LT,
				StopCmp:  database.OP_GT,
				Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
				Stop:     []database.Cell{{Type: database.TypeI64, I64: j}},
			}
			testReq(req, i-1, j+1, true)
		}
	}

	for i := int64(-1); i < N+1; i++ {
		req := &database.RangeReq{
			StartCmp: database.OP_GE,
			StopCmp:  database.OP_LE,
			Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
			Stop:     nil,
		}
		testReq(req, i, N, false)

		req = &database.RangeReq{
			StartCmp: database.OP_LE,
			StopCmp:  database.OP_GE,
			Start:    []database.Cell{{Type: database.TypeI64, I64: i}},
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

func TestTableExpr(t *testing.T) {
	db := &database.DB{}
	exec := &Exec{DB: db}
	db.KV.Options.Dirpath = "test_db_table_expr"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := `
		create table t (
			a int64, b int64, c string, d string,
			index (b),
			index (a, d),
			primary key (d)
		);
	`
	_, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	schema, err := db.GetSchema("t")
	require.Nil(t, err)
	expected := database.Schema{
		Table: "t",
		Cols:  []database.Column{{"a", database.TypeI64}, {"b", database.TypeI64}, {"c", database.TypeStr}, {"d", database.TypeStr}},
		Indices: [][]int{
			{3},
			{1, 3},
			{0, 3},
		},
	}
	assert.Equal(t, expected, schema)

	s = "insert into t values (1, 2, 'a', 'b');"
	r, err := exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a * 4 - b, d + c from t where d = 'b';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(2, "ba")}, r.Values)

	s = "update t set a = a - b, b = a, c = d + c where d = 'b';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a, b, c, d from t where d = 'b';"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(-1, 1, "ba", "b")}, r.Values)
}

func TestTableIndices(t *testing.T) {
	db := &database.DB{}
	exec := &Exec{DB: db}
	db.KV.Options.Dirpath = "test_db_table_indices"
	defer os.RemoveAll(db.KV.Options.Dirpath)

	os.RemoveAll(db.KV.Options.Dirpath)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := `
		create table t (
			a int64, b int64,
			index (b),
			primary key (a)
		);
	`
	_, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	schema, err := db.GetSchema("t")
	require.Nil(t, err)
	expected := database.Schema{
		Table: "t",
		Cols:  []database.Column{{"a", database.TypeI64}, {"b", database.TypeI64}},
		Indices: [][]int{
			{0},
			{1, 0},
		},
	}
	assert.Equal(t, expected, schema)

	s = "insert into t values (1, 2);"
	r, err := exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 1)
	s = "insert into t values (2, 2);"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 1)
	s = "insert into t values (0, 3);"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 1)
	s = "insert into t values (1, 2);"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 0)
	// (1, 2), (2, 2), (0, 3)

	s = "select a, b from t where a >= 0;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(0, 3), makeRow(1, 2), makeRow(2, 2)}, r.Values)

	s = "select a, b from t where b > 2;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(0, 3)}, r.Values)

	s = "select a, b from t where b >= 2;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(1, 2), makeRow(2, 2), makeRow(0, 3)}, r.Values)

	s = "update t set b = b - a where b < 3;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 2)
	// (1, 1), (2, 0), (0, 3)

	s = "select a, b from t where a >= 0;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(0, 3), makeRow(1, 1), makeRow(2, 0)}, r.Values)

	s = "select a, b from t where b < 3;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(1, 1), makeRow(2, 0)}, r.Values)

	s = "delete from t where b >= 1;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.True(t, err == nil && r.Updated == 2)
	// (2, 0)

	s = "select a, b from t where a >= 0;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(2, 0)}, r.Values)

	s = "select a, b from t where b >= 0;"
	r, err = exec.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(2, 0)}, r.Values)
}
