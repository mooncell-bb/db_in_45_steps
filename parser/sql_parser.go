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
	keys  []database.NamedCell
}

type StmtCreatTable struct {
	table string
	cols  []database.Column
	pkey  []string
}

type StmtInsert struct {
	table string
	value []database.Cell
}

type StmtUpdate struct {
	table string
	keys  []database.NamedCell
	value []database.NamedCell
}

type StmtDelete struct {
	table string
	keys  []database.NamedCell
}

type ExprBinOp struct {
	op    database.ExprOp
	left  any
	right any
}

type ExprUnOp struct {
	op  database.ExprOp
	kid any
}

func (p *Parser) ParseStmt() (out any, err error) {
	if p.tryKeyword("SELECT") {
		stmt := &StmtSelect{}
		err = p.parseSelect(stmt)
		out = stmt
	} else if p.tryKeyword("CREATE", "TABLE") {
		stmt := &StmtCreatTable{}
		err = p.parseCreateTable(stmt)
		out = stmt
	} else if p.tryKeyword("INSERT", "INTO") {
		stmt := &StmtInsert{}
		err = p.parseInsert(stmt)
		out = stmt
	} else if p.tryKeyword("UPDATE") {
		stmt := &StmtUpdate{}
		err = p.parseUpdate(stmt)
		out = stmt
	} else if p.tryKeyword("DELETE", "FROM") {
		stmt := &StmtDelete{}
		err = p.parseDelete(stmt)
		out = stmt
	} else {
		err = errors.New("unknown statement")
	}

	if err != nil {
		return nil, err
	}

	return out, nil
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

func (p *Parser) tryKeyword(kws ...string) bool {
	if p.isEnd() {
		return false
	}

	save := p.pos
	for _, kw := range kws {
		p.skipSpaces()

		if !(p.pos+len(kw) <= len(p.buf) && strings.EqualFold(p.buf[p.pos:p.pos+len(kw)], kw)) {
			p.pos = save
			return false
		}

		if p.pos+len(kw) < len(p.buf) && !IsSeparator(p.buf[p.pos+len(kw)]) {
			p.pos = save
			return false
		}

		p.pos += len(kw)
	}

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

func (p *Parser) parseEqual(out *database.NamedCell) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect column")
	} else {
		out.Column = name
	}

	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}

	return p.parseValue(&out.Value)
}

func (p *Parser) parseWhere(out *[]database.NamedCell) error {
	if !p.tryKeyword("WHERE") {
		return errors.New("expect keyword")
	}

	for !p.tryPunctuation(";") {
		expr := database.NamedCell{}

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

func (p *Parser) parseDelete(out *StmtDelete) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	return p.parseWhere(&out.keys)
}

func (p *Parser) parseUpdate(out *StmtUpdate) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	if !p.tryKeyword("SET") {
		return errors.New("expect SET")
	}

	for !p.tryKeyword("WHERE") {
		expr := database.NamedCell{}

		if len(out.value) > 0 && !p.tryKeyword(",") {
			return errors.New("expect ,")
		}

		if err := p.parseEqual(&expr); err != nil {
			return err
		}

		out.value = append(out.value, expr)
	}

	if len(out.value) == 0 {
		return errors.New("expect assignment list")
	}

	p.pos -= len("WHERE")
	return p.parseWhere(&out.keys)
}

func (p *Parser) parseCommaList(item func() error) error {
	if !p.tryPunctuation("(") {
		return errors.New("expect (")
	}

	comma := false
	for !p.tryPunctuation(")") {
		if comma && !p.tryPunctuation(",") {
			return errors.New("expect ,")
		}

		comma = true
		if err := item(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseValueItem(out *[]database.Cell) error {
	cell := database.Cell{}

	if err := p.parseValue(&cell); err != nil {
		return err
	}

	*out = append(*out, cell)

	return nil
}

func (p *Parser) parseInsert(out *StmtInsert) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	if !p.tryKeyword("VALUES") {
		return errors.New("expect VALUES")
	}

	err := p.parseCommaList(
		func() error {
			return p.parseValueItem(&out.value)
		},
	)

	if err != nil {
		return err
	}

	if !p.tryPunctuation(";") {
		return errors.New("expect ;")
	}

	return nil
}

func (p *Parser) parseNameItem(out *[]string) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect name")
	} else {
		*out = append(*out, name)
	}

	return nil
}

func (p *Parser) parseCreateTableItem(out *StmtCreatTable) error {
	if p.tryKeyword("PRIMARY", "KEY") {
		return p.parseCommaList(
			func() error {
				return p.parseNameItem(&out.pkey)
			},
		)
	}

	col := database.Column{}
	if name, ok := p.tryName(); !ok {
		return errors.New("expect name")
	} else {
		col.Name = name
	}

	if kind, ok := p.tryName(); !ok {
		return errors.New("expect name")
	} else {
		switch kind {
		case "int64":
			col.Type = database.TypeI64
		case "string":
			col.Type = database.TypeStr
		default:
			return errors.New("unkown column type")
		}
	}

	out.cols = append(out.cols, col)
	return nil
}

func (p *Parser) parseCreateTable(out *StmtCreatTable) error {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	err := p.parseCommaList(
		func() error {
			return p.parseCreateTableItem(out)
		},
	)

	if err != nil {
		return err
	}

	if !p.tryPunctuation(";") {
		return errors.New("expect ;")
	}

	return nil
}

func (p *Parser) parseAtom() (expr any, err error) {
	if p.tryPunctuation("(") {
		if expr, err = p.ParseExpr(); err != nil {
			return nil, err
		}

		if !p.tryPunctuation(")") {
			return nil, errors.New("expect )")
		}

		return expr, nil
	}

	if name, ok := p.tryName(); ok {
		return name, nil
	}

	cell := &database.Cell{}
	if err := p.parseValue(cell); err != nil {
		return nil, err
	}

	return cell, nil
}

func (p *Parser) parseBinop(tokens []string, ops []database.ExprOp, inner func() (any, error)) (any, error) {
	if len(tokens) != len(ops) {
		panic("params mismatch")
	}

	left, err := inner()
	if err != nil {
		return nil, err
	}

	for ok := true; ok; {
		ok = false
		for idx, token := range tokens {
			if !p.tryPunctuation(token) && !p.tryKeyword(token) {
				continue
			}

			ok = true
			right, err := inner()
			if err != nil {
				return nil, err
			}

			left = &ExprBinOp{
				op:    ops[idx],
				left:  left,
				right: right,
			}

			break
		}
	}

	return left, nil
}

func (p *Parser) parseNeg() (expr any, err error) {
	if p.tryPunctuation("-") {
		if expr, err = p.parseNeg(); err != nil {
			return nil, err
		}
		return &ExprUnOp{
			op:  database.OP_NEG,
			kid: expr,
		}, nil
	} else {
		return p.parseAtom()
	}
}

func (p *Parser) parseMul() (any, error) {
	tokens := []string{"*", "/"}
	ops := []database.ExprOp{database.OP_MUL, database.OP_DIV}
	return p.parseBinop(tokens, ops, p.parseNeg)
}

func (p *Parser) parseAdd() (any, error) {
	tokens := []string{"+", "-"}
	ops := []database.ExprOp{database.OP_ADD, database.OP_SUB}
	return p.parseBinop(tokens, ops, p.parseMul)
}

func (p *Parser) parseCmp() (any, error) {
	tokens := []string{"=", "!=", "<>", "<=", ">=", "<", ">"}
	ops := []database.ExprOp{
		database.OP_EQ,
		database.OP_NE,
		database.OP_NE,
		database.OP_LE,
		database.OP_GE,
		database.OP_LT,
		database.OP_GT,
	}
	return p.parseBinop(tokens, ops, p.parseAdd)
}

func (p *Parser) parseNot() (expr any, err error) {
	if p.tryKeyword("NOT") {
		if expr, err = p.parseNot(); err != nil {
			return nil, err
		}
		return &ExprUnOp{
			op:  database.OP_NOT,
			kid: expr,
		}, nil
	} else {
		return p.parseCmp()
	}
}

func (p *Parser) parseAnd() (any, error) {
	tokens := []string{"AND"}
	ops := []database.ExprOp{database.OP_AND}
	return p.parseBinop(tokens, ops, p.parseNot)
}

func (p *Parser) parseOr() (any, error) {
	tokens := []string{"OR"}
	ops := []database.ExprOp{database.OP_OR}
	return p.parseBinop(tokens, ops, p.parseAnd)
}

func (p *Parser) ParseExpr() (any, error) {
	return p.parseOr()
}
