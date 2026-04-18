package parser

import (
	"testing"

	"github.com/mooncell-bb/db_in_45_steps/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseName(t *testing.T) {
	p := NewParser(" a b0 _0_ 123 ")
	name, ok := p.tryName()
	assert.True(t, ok && name == "a")
	name, ok = p.tryName()
	assert.True(t, ok && name == "b0")
	name, ok = p.tryName()
	assert.True(t, ok && name == "_0_")
	_, ok = p.tryName()
	assert.False(t, ok)
}

func TestParseNameEdgeCases(t *testing.T) {
	// Name starting with underscore
	p := NewParser("_abc")
	name, ok := p.tryName()
	assert.True(t, ok && name == "_abc")

	// Single character name
	p = NewParser("x ")
	name, ok = p.tryName()
	assert.True(t, ok && name == "x")

	// Name at end of string (no space)
	p = NewParser("test")
	name, ok = p.tryName()
	assert.True(t, ok && name == "test")

	// Empty input
	p = NewParser("")
	_, ok = p.tryName()
	assert.False(t, ok)

	// Only spaces
	p = NewParser("   ")
	_, ok = p.tryName()
	assert.False(t, ok)

	// Starting with digit (invalid)
	p = NewParser("123abc")
	_, ok = p.tryName()
	assert.False(t, ok)

	// Starting with special character (invalid)
	p = NewParser("@name")
	_, ok = p.tryName()
	assert.False(t, ok)
}

func TestParseNameMultiple(t *testing.T) {
	p := NewParser("user_id product_name _internal")

	// First name
	name, ok := p.tryName()
	assert.True(t, ok && name == "user_id")

	// Second name
	name, ok = p.tryName()
	assert.True(t, ok && name == "product_name")

	// Third name
	name, ok = p.tryName()
	assert.True(t, ok && name == "_internal")

	// No more names
	_, ok = p.tryName()
	assert.False(t, ok)
}

func TestParseKeyword(t *testing.T) {
	p := NewParser(" select  HELLO ")
	assert.False(t, p.tryKeyword("sel"))
	assert.True(t, p.tryKeyword("SELECT"))
	assert.True(t, p.tryKeyword("hello") && p.isEnd())

	p = NewParser(" select  HELLO ")
	assert.False(t, p.tryKeyword("select", "hi"))
	assert.True(t, p.tryKeyword("select", "hello") && p.isEnd())
}

func TestParseKeywordEdgeCases(t *testing.T) {
	// Case insensitive matching
	p := NewParser("SELECT ")
	assert.True(t, p.tryKeyword("select"))
	assert.True(t, p.isEnd())

	// Keyword must be followed by separator
	p = NewParser("selectfrom")
	assert.False(t, p.tryKeyword("select"))
	assert.Equal(t, 0, p.pos) // Position should not change

	// Keyword followed by space and punctuation
	p = NewParser("insert ( id )")
	assert.True(t, p.tryKeyword("INSERT"))
	assert.True(t, p.tryPunctuation("("))
	name, ok := p.tryName()
	assert.True(t, ok && name == "id")
	assert.True(t, p.tryPunctuation(")"))

	// Partial keyword match fails
	p = NewParser("upd values")
	assert.False(t, p.tryKeyword("update"))
	assert.Equal(t, 0, p.pos)

	// Empty string
	p = NewParser("")
	assert.False(t, p.tryKeyword("select"))

	// Keyword with leading/trailing spaces
	p = NewParser("  FROM  ")
	assert.True(t, p.tryKeyword("from"))
	assert.True(t, p.isEnd())
}

func TestParseKeywordMultiple(t *testing.T) {
	p := NewParser("SELECT * FROM table WHERE id = 1")
	assert.True(t, p.tryKeyword("SELECT"))
	assert.False(t, p.tryPunctuation("("))
	assert.True(t, p.tryPunctuation("*"))
	assert.True(t, p.tryKeyword("FROM"))
	name, ok := p.tryName()
	assert.True(t, ok && name == "table")
	assert.True(t, p.tryKeyword("WHERE"))
	name, ok = p.tryName()
	assert.True(t, ok && name == "id")
}

func TestParsePunctuation(t *testing.T) {
	p := NewParser(" ( ) , * ")
	assert.True(t, p.tryPunctuation("("))
	assert.True(t, p.tryPunctuation(")"))
	assert.True(t, p.tryPunctuation(","))
	assert.True(t, p.tryPunctuation("*"))
	assert.False(t, p.tryPunctuation(";"))
}

func TestParsePunctuationEdgeCases(t *testing.T) {
	// Single character punctuation
	p := NewParser(";")
	assert.True(t, p.tryPunctuation(";"))
	assert.True(t, p.isEnd())

	// Multi-character punctuation
	p = NewParser("<=")
	assert.True(t, p.tryPunctuation("<="))
	assert.True(t, p.isEnd())

	// Punctuation with spaces
	p = NewParser("  [ ]  ")
	assert.True(t, p.tryPunctuation("["))
	assert.True(t, p.tryPunctuation("]"))
	assert.True(t, p.isEnd())

	// Mismatch in multi-char punctuation
	p = NewParser("<>")
	assert.False(t, p.tryPunctuation("<="))
	assert.Equal(t, 0, p.pos)

	// Empty string
	p = NewParser("")
	assert.False(t, p.tryPunctuation("("))

	// Only spaces
	p = NewParser("   ")
	assert.False(t, p.tryPunctuation(")"))
}

func TestParsePunctuationVariations(t *testing.T) {
	tests := []string{"(", ")", "[", "]", "{", "}", ",", ";", "=", "*", "/", "+", "-"}

	for _, punct := range tests {
		p := NewParser(punct)
		assert.True(t, p.tryPunctuation(punct), "Failed to parse punctuation: %s", punct)
		assert.True(t, p.isEnd())
	}
}

func testParseValue(t *testing.T, s string, ref database.Cell) {
	p := NewParser(s)
	out := database.Cell{}
	err := p.parseValue(&out)
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseValue(t *testing.T) {
	testParseValue(t, " -123 ", database.Cell{Type: database.TypeI64, I64: -123})
	testParseValue(t, ` 'abc\'\"d' `, database.Cell{Type: database.TypeStr, Str: []byte("abc'\"d")})
	testParseValue(t, ` "abc\'\"d" `, database.Cell{Type: database.TypeStr, Str: []byte("abc'\"d")})
}

func testParseSelect(t *testing.T, s string, ref StmtSelect) {
	p := NewParser(s)
	assert.True(t, p.tryKeyword("SELECT"))
	out := StmtSelect{}
	err := p.parseSelect(&out)
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseSelect(t *testing.T) {
	s := "select a from t where c=1;"
	stmt := StmtSelect{
		table: "t",
		cols:  []string{"a"},
		keys:  []database.NamedCell{{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 1}}},
	}
	testParseSelect(t, s, stmt)

	s = "select a,b_02 from T where c=1 and d='e';"
	stmt = StmtSelect{
		table: "T",
		cols:  []string{"a", "b_02"},
		keys: []database.NamedCell{
			{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 1}},
			{Column: "d", Value: database.Cell{Type: database.TypeStr, Str: []byte("e")}},
		},
	}
	testParseSelect(t, s, stmt)

	s = "select a, b_02 from T where c = 1 and d = 'e' ; "
	testParseSelect(t, s, stmt)

	s = `SELECT x, y FROM users WHERE age = 42 AND name = "bob";`
	stmt = StmtSelect{
		table: "users",
		cols:  []string{"x", "y"},
		keys: []database.NamedCell{
			{Column: "age", Value: database.Cell{Type: database.TypeI64, I64: 42}},
			{Column: "name", Value: database.Cell{Type: database.TypeStr, Str: []byte("bob")}},
		},
	}
	testParseSelect(t, s, stmt)
}

func testParseSelectError(t *testing.T, s string) {
	p := NewParser(s)
	assert.True(t, p.tryKeyword("SELECT"))
	out := StmtSelect{}
	err := p.parseSelect(&out)
	assert.NotNil(t, err)
}

func TestParseSelectFailures(t *testing.T) {
	testParseSelectError(t, "select from t where c=1;")
	testParseSelectError(t, "select a t where c=1;")
	testParseSelectError(t, "select a from where c=1;")
	testParseSelectError(t, "select a from t;")
	testParseSelectError(t, "select a from t where c=1")
	testParseSelectError(t, "select a from t where c=1 d=2;")
}

func TestParserCombined(t *testing.T) {
	// Test a more complex SQL-like statement
	p := NewParser("CREATE TABLE users ( id INT , name VARCHAR(50) )")

	assert.True(t, p.tryKeyword("CREATE"))
	assert.True(t, p.tryKeyword("TABLE"))

	name, ok := p.tryName()
	assert.True(t, ok && name == "users")

	assert.True(t, p.tryPunctuation("("))

	name, ok = p.tryName()
	assert.True(t, ok && name == "id")

	typeVal, ok := p.tryName()
	assert.True(t, ok && typeVal == "INT")

	assert.True(t, p.tryPunctuation(","))

	name, ok = p.tryName()
	assert.True(t, ok && name == "name")

	typeVal, ok = p.tryName()
	assert.True(t, ok && typeVal == "VARCHAR")

	assert.True(t, p.tryPunctuation("("))
	assert.True(t, p.tryPunctuation("50"))
	assert.True(t, p.tryPunctuation(")"))

	assert.True(t, p.tryPunctuation(")"))
	assert.True(t, p.isEnd())
}

func testParseStmt(t *testing.T, s string, ref any) {
	p := NewParser(s)
	out, err := p.ParseStmt()
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseStmt(t *testing.T) {
	var stmt any
	s := "select a from t where c=1;"
	stmt = &StmtSelect{
		table: "t",
		cols:  []string{"a"},
		keys:  []database.NamedCell{{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 1}}},
	}
	testParseStmt(t, s, stmt)

	s = "select a,b_02 from T where c=1 and d='e';"
	stmt = &StmtSelect{
		table: "T",
		cols:  []string{"a", "b_02"},
		keys: []database.NamedCell{
			{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 1}},
			{Column: "d", Value: database.Cell{Type: database.TypeStr, Str: []byte("e")}},
		},
	}
	testParseStmt(t, s, stmt)

	s = "select a, b_02 from T where c = 1 and d = 'e' ; "
	testParseStmt(t, s, stmt)

	s = "create table t (a string, b int64, primary key (b));"
	stmt = &StmtCreatTable{
		table: "t",
		cols:  []database.Column{{Name: "a", Type: database.TypeStr}, {Name: "b", Type: database.TypeI64}},
		pkey:  []string{"b"},
	}
	testParseStmt(t, s, stmt)

	s = "insert into t values (1, 'hi');"
	stmt = &StmtInsert{
		table: "t",
		value: []database.Cell{{Type: database.TypeI64, I64: 1}, {Type: database.TypeStr, Str: []byte("hi")}},
	}
	testParseStmt(t, s, stmt)

	s = "update t set a = 1, b = 2 where c = 3 and d = 4;"
	stmt = &StmtUpdate{
		table: "t",
		value: []database.NamedCell{{Column: "a", Value: database.Cell{Type: database.TypeI64, I64: 1}}, {Column: "b", Value: database.Cell{Type: database.TypeI64, I64: 2}}},
		keys:  []database.NamedCell{{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 3}}, {Column: "d", Value: database.Cell{Type: database.TypeI64, I64: 4}}},
	}
	testParseStmt(t, s, stmt)

	s = "delete from t where c = 3 and d = 4;"
	stmt = &StmtDelete{
		table: "t",
		keys:  []database.NamedCell{{Column: "c", Value: database.Cell{Type: database.TypeI64, I64: 3}}, {Column: "d", Value: database.Cell{Type: database.TypeI64, I64: 4}}},
	}
	testParseStmt(t, s, stmt)
}

func testParseExpr(t *testing.T, s string, expr any) {
	p := NewParser(s)
	out, err := p.ParseExpr()
	require.Nil(t, err)
	assert.Equal(t, expr, out)
	assert.True(t, p.isEnd())
}

func TestParseExpr(t *testing.T) {
	var expr any

	testParseExpr(t, "a", "a")
	testParseExpr(t, "(a)", "a")
	testParseExpr(t, "1", &database.Cell{Type: database.TypeI64, I64: 1})

	s := "a + 1"
	expr = &ExprBinOp{op: database.OP_ADD, left: "a", right: &database.Cell{Type: database.TypeI64, I64: 1}}
	testParseExpr(t, s, expr)

	s = "a + 1 - b"
	expr = &ExprBinOp{op: database.OP_SUB,
		left:  &ExprBinOp{op: database.OP_ADD, left: "a", right: &database.Cell{Type: database.TypeI64, I64: 1}},
		right: "b",
	}
	testParseExpr(t, s, expr)

	s = "a + b * c"
	expr = &ExprBinOp{op: database.OP_ADD,
		left:  "a",
		right: &ExprBinOp{op: database.OP_MUL, left: "b", right: "c"},
	}
	testParseExpr(t, s, expr)

	s = "(a * b)"
	expr = &ExprBinOp{op: database.OP_MUL, left: "a", right: "b"}
	testParseExpr(t, s, expr)

	s = "(a + b) / c"
	expr = &ExprBinOp{op: database.OP_DIV,
		left:  &ExprBinOp{op: database.OP_ADD, left: "a", right: "b"},
		right: "c",
	}
	testParseExpr(t, s, expr)

	s = "f or e and not d = a + b * -c"
	expr = &ExprBinOp{op: database.OP_OR,
		left: "f", right: &ExprBinOp{op: database.OP_AND,
			left: "e", right: &ExprUnOp{op: database.OP_NOT,
				kid: &ExprBinOp{op: database.OP_EQ,
					left: "d", right: &ExprBinOp{op: database.OP_ADD,
						left: "a", right: &ExprBinOp{op: database.OP_MUL,
							left: "b", right: &ExprUnOp{op: database.OP_NEG, kid: "c"}}}}}}}
	testParseExpr(t, s, expr)

	s = "not not - - a"
	expr = &ExprUnOp{op: database.OP_NOT,
		kid: &ExprUnOp{op: database.OP_NOT,
			kid: &ExprUnOp{op: database.OP_NEG,
				kid: &ExprUnOp{op: database.OP_NEG, kid: "a"}}}}
	testParseExpr(t, s, expr)
}
