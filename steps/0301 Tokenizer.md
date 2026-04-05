string -> tokens -> struct

SQL 字符串必须先解析为程序数据才能进行处理。

```sql
SELECT a, b FROM t WHERE c = 1;
```

```go
StmtSelect{
    table: "t",
    cols:  []string{"a", "b"},
    keys:  []NamedCell{{column: "c", value: Cell{Type: TypeI64, I64: 1}}},
}
```

SQL tokens 可以分为以下几类：

- 关键字（Keywords）：SELECT、FROM、WHERE……
- 名称（Names）：表名、列名等，a、b、t、c……
- 符号（Symbols）："="、";"、"("、")"……
- 取值（Numbers、Strings）：1、'hello'……

```sql
CREATE TABLE t (a int64, b int64, c string, d string, PRIMARY KEY (a, b));

INSERT INTO t VALUES (1, 2, 'x', 'y');
DELETE FROM t WHERE c = 'x' AND d = 'y';
UPDATE t SET a = 1, b = 2 WHERE c = 'x' AND d = 'y';
SELECT a, b FROM t WHERE b = 1 AND c = 'x';
```

新建 parser 文件夹，同时创建 sql_parser_utils.go 文件，其中存放解析字符串的工具函数：

```
parser                  
└─ sql_parser_utils.go
```

```go
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
```

在 parser 文件夹下创建 sql_parse.go，其中定义 Parser 结构体，用于解析字符串：

```
parser                  
├─ sql_parse.go         
└─ sql_parser_utils.go
```

```go
type Parser struct {
    buf string
    pos int
}

func NewParser(s string) Parser {
    return Parser{buf: s, pos: 0}
}
```

Parser 持有需要解析的字符串 buf，以及已经解析的位置长度 pos。

实现 Parser.tryName()、Parser.tryKeyword() 和 Parser.tryPunctuation() 方法，分别用于解析单个名称、解析给定关键字和给定词元：

- func (p *Parser) tryName() (string, bool)
- func (p *Parser) tryKeyword(kw string) bool
- func (p *Parser) tryPunctuation(tok string) bool

Parser.tryName() 方法解析的首字符为字母或 _，后续字符为字母、数字或 _，若成功，返回 true 并推进 pos。例如 Parser {buf: " hi ", pos: 0} 经过 Parser.tryName() 方法解析后返回 "hi"，pos = 3。

Parser.tryKeyword() 方法匹配的关键词不区分大小写，关键词必须由空格或标点符号分隔。例如只有 "SELECT abc" 中匹配 "SELECT" 时才算关键词，而 "SELECTabc" 则不算。strings.EqualFold() 函数提供不区分大小写的字符串比较。

Parser.tryPunctuation() 方法匹配的词元必须严格匹配（区分大小写），匹配成功即可返回 true 并增进 pos。

使用给定方法 Parser.skipSpaces() 和 Parser.isEnd()，分别用于调过前导空格和判断是否解析结束。

```go
func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && IsSpace(p.buf[p.pos]) {
		p.pos++
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}
```

Parser.tryName() 方法用于解析表名、列名，例如解析 a、b、t、c 等。

Parser.tryKeyword() 方法尝试匹配给定关键词，例如匹配 SELECT、FROM、WHERE 等。

Parser.tryPunctuation() 方法可以尝试匹配特定结构，例如匹配 =、;、int64、string 等特定字符。