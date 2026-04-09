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
}
