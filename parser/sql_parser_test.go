package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
