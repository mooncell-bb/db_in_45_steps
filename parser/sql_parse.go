package parser

import "strings"

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
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
