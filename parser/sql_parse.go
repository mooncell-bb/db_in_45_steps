package parser

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/mooncell-bb/db_in_45_steps/database"
)

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

type StmtSelect struct {
	table string
	cols  []string
	keys  []NamedCell
}

type NamedCell struct {
	column string
	value  database.Cell
}

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && IsSpace(p.buf[p.pos]) {
		p.pos++
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}

func (p *Parser) tryName() (string, bool) {
	if p.isEnd() {
		return "", false
	}

	if !IsNameStart(p.buf[p.pos]) {
		return "", false
	}

	end := p.pos + 1
	for end < len(p.buf) && IsNameContinue(p.buf[end]) {
		end++
	}

	str := p.buf[p.pos:end]
	p.pos = end

	return str, true
}

func (p *Parser) tryKeyword(kw string) bool {
	if p.isEnd() {
		return false
	}

	if p.pos+len(kw) > len(p.buf) {
		return false
	}

	keyword := p.buf[p.pos : p.pos+len(kw)]
	if !strings.EqualFold(keyword, kw) {
		return false
	}

	n := p.pos + len(kw)
	if n >= len(p.buf) || !IsSeparator(p.buf[n]) {
		return false
	}

	p.pos += len(kw)

	return true
}

func (p *Parser) tryPunctuation(tok string) bool {
	if p.isEnd() {
		return false
	}

	if p.pos+len(tok) > len(p.buf) {
		return false
	}

	token := p.buf[p.pos : p.pos+len(tok)]
	if token != tok {
		return false
	}

	p.pos += len(tok)

	return true
}

func (p *Parser) parseValue(out *database.Cell) error {
	if p.isEnd() {
		return errors.New("expect value")
	}

	ch := p.buf[p.pos]
	if ch == '"' || ch == '\'' {
		return p.parseString(out)
	} else if IsDigit(ch) || ch == '-' || ch == '+' {
		return p.parseInt(out)
	} else {
		return errors.New("expect value")
	}
}

func (p *Parser) parseInt(out *database.Cell) error {
	end := p.pos

	if p.buf[end] == '-' || p.buf[end] == '+' {
		end++
	}

	for end < len(p.buf) && IsDigit(p.buf[end]) {
		end++
	}

	num, err := strconv.ParseInt(p.buf[p.pos:end], 10, 64)
	if err != nil {
		return err
	}

	out.Type = database.TypeI64
	out.I64 = num
	p.pos = end

	return nil
}

func (p *Parser) parseString(out *database.Cell) error {
	match := p.buf[p.pos]

	var str bytes.Buffer
	str.Grow(64)

	end := p.pos + 1
	for end < len(p.buf) {
		ch := p.buf[end]
		switch ch {
		case '\\':
			end++
			if end < len(p.buf) && (p.buf[end] == '"' || p.buf[end] == '\'') {
				str.WriteByte(p.buf[end])
				end++
			} else {
				return errors.New("bad escape")
			}
		case match:
			out.Type = database.TypeStr
			out.Str = str.Bytes()
			p.pos = end + 1
			return nil
		default:
			str.WriteByte(ch)
			end++
		}
	}

	return errors.New("string is not terminated")
}

func (p *Parser) parseEqual(out *NamedCell) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect column")
	} else {
		out.column = name
	}

	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}

	return p.parseValue(&out.value)
}

func (p *Parser) parseWhere(out *[]NamedCell) error {
	if !p.tryKeyword("WHERE") {
		return errors.New("expect keyword")
	}

	for !p.tryPunctuation(";") {
		expr := NamedCell{}

		if len(*out) > 0 && !p.tryKeyword("AND") {
			return errors.New("expect AND")
		}

		if err := p.parseEqual(&expr); err != nil {
			return err
		}

		*out = append(*out, expr)
	}

	if len(*out) == 0 {
		return errors.New("expect where clause")
	}

	return nil
}

func (p *Parser) parseSelect(out *StmtSelect) error {
	if !p.tryKeyword("SELECT") {
		return errors.New("expect keyword")
	}

	for !p.tryKeyword("FROM") {
		if len(out.cols) > 0 && !p.tryPunctuation(",") {
			return errors.New("expect comma")
		}

		if name, ok := p.tryName(); ok {
			out.cols = append(out.cols, name)
		} else {
			return errors.New("expect column")
		}
	}

	if len(out.cols) == 0 {
		return errors.New("expect column list")
	}

	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	return p.parseWhere(&out.keys)
}
