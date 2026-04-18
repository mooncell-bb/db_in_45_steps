package parser

import (
	"testing"

	"github.com/mooncell-bb/db_in_45_steps/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testEval(t *testing.T, schema *database.Schema, row database.Row, s string, expected database.Cell) {
	p := NewParser(s)
	expr, err := p.ParseExpr()
	require.Nil(t, err)
	require.True(t, p.isEnd())

	out, err := evalExpr(schema, row, expr)
	require.Nil(t, err)
	assert.Equal(t, expected, *out)
}

func makeCell(v any) database.Cell {
	switch val := v.(type) {
	case int:
		return database.Cell{Type: database.TypeI64, I64: int64(val)}
	case string:
		return database.Cell{Type: database.TypeStr, Str: []byte(val)}
	default:
		panic("unreachable")
	}
}

func makeRow(vs ...any) (row database.Row) {
	for _, v := range vs {
		row = append(row, makeCell(v))
	}
	return row
}

func TestEval(t *testing.T) {
	schema := &database.Schema{
		Table: "t",
		Cols: []database.Column{
			{Name: "a", Type: database.TypeStr},
			{Name: "b", Type: database.TypeStr},
			{Name: "c", Type: database.TypeI64},
			{Name: "d", Type: database.TypeI64},
		},
		PKey: []int{0},
	}

	row := makeRow("A", "B", 3, 4)
	testEval(t, schema, row, "a + b", makeCell("AB"))
	testEval(t, schema, row, "c - d", makeCell(-1))
	testEval(t, schema, row, "c * d - d * c + d", makeCell(4))
	testEval(t, schema, row, "d or c and not d = c", makeCell(1))
}
