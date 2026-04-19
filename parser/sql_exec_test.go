package parser

import (
	"os"
	"testing"

	"github.com/mooncell-bb/db_in_45_steps/database"
	"github.com/stretchr/testify/require"
)

func parseStmt(t *testing.T, s string) any {
	p := NewParser(s)
	stmt, err := p.ParseStmt()
	require.Nil(t, err)
	return stmt
}

func TestExecStmt(t *testing.T) {
	db := &database.DB{}
	exec := &Exec{DB: db}
	db.KV.Log.FileName = ".test_db"
	defer os.Remove(".test_db")

	exec.Open()
	defer exec.Close()

	os.Remove(".test_db")

	s := "create table link (time int64, src string, dst string, primary key (src, dst));"
	stmt := parseStmt(t, s)
	_, err := exec.ExecStmt(stmt)
	require.Nil(t, err)

	s = "insert into link values (123, 'bob', 'alice');"
	stmt = parseStmt(t, s)
	r, err := exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	stmt = parseStmt(t, s)
	r, err = exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, []database.Row{{database.Cell{Type: database.TypeI64, I64: 123}}}, r.Values)

	s = "update link set time = 456 where dst = 'alice' and src = 'bob';"
	stmt = parseStmt(t, s)
	r, err = exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	stmt = parseStmt(t, s)
	r, err = exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, []database.Row{{database.Cell{Type: database.TypeI64, I64: 456}}}, r.Values)

	err = exec.Close()

	db = &database.DB{}
	exec = &Exec{DB: db}
	db.KV.Log.FileName = ".test_db"
	exec.Open()
	defer exec.Close()
	require.Nil(t, err)

	s = "delete from link where src = 'bob' and dst = 'alice';"
	stmt = parseStmt(t, s)
	r, err = exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	stmt = parseStmt(t, s)
	r, err = exec.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 0, len(r.Values))

	db2 := &database.DB{}
	exec2 := &Exec{DB: db2}
	db2.KV.Log.FileName = ".test_db"
	os.Remove(".test_db")
	exec2.Open()
	defer exec2.Close()

	s = "create table t (a int64, b int64, c string, d string, primary key (d));"
	stmt = parseStmt(t, s)
	_, err = exec2.ExecStmt(stmt)
	require.Nil(t, err)

	s = "insert into t values (1, 2, 'a', 'b');"
	stmt = parseStmt(t, s)
	r, err = exec2.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a * 4 - b, d + c from t where d = 'b';"
	stmt = parseStmt(t, s)
	r, err = exec2.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(2, "ba")}, r.Values)

	s = "update t set a = a - b, b = a, c = d + c where d = 'b';"
	stmt = parseStmt(t, s)
	r, err = exec2.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a, b, c, d from t where d = 'b';"
	stmt = parseStmt(t, s)
	r, err = exec2.ExecStmt(stmt)
	require.Nil(t, err)
	require.Equal(t, []database.Row{makeRow(-1, 1, "ba", "b")}, r.Values)
}
