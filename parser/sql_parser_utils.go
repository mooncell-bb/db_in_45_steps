package parser

func IsSpace(ch byte) bool {
	switch ch {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

func IsAlpha(ch byte) bool {
	return 'a' <= (ch|32) && (ch|32) <= 'z'
}

func IsDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func IsNameStart(ch byte) bool {
	return IsAlpha(ch) || ch == '_'
}

func IsNameContinue(ch byte) bool {
	return IsAlpha(ch) || IsDigit(ch) || ch == '_'
}

func IsSeparator(ch byte) bool {
	return ch < 128 && !IsNameContinue(ch)
}
