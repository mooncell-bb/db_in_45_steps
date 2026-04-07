SELECT 语句的形式为：SELECT a, b FROM t WHERE c = 1 AND d = 'e';

可以将其转化为数据结构表示：

```
StmtSelect{
    table: "t",
    cols: []string{"a", "b"},
    keys: []NamedCell{
        {column: "c", value: Cell{Type: TypeI64, I64: 1}},
        {column: "d", value: Cell{Type: TypeStr, Str: []byte("e")}},
    },
}
```

parse/sql_parse.go 添加 StmtSelect 结构体，用来存储解析的 SELECT 语句：

```go
type StmtSelect struct {
	table string
	cols  []string
	keys  []NamedCell
}

type NamedCell struct {
	column string
	value  database.Cell
}
```

观察到 SELECT 语句由关键字 SELECT、列名 a, b、关键字 FROM、表名 t 等组成，因此可以使用之前实现的 Parser.tryKeyword()、Parser.tryName() 等方法依次解析。

首先对最小单元 c = 1、d = 'e' 结构进行解析，其可以通过 Parser.tryName()、Parser.tryPunctuation("=") 和 Parser.parseValue() 方法依次解析，因此实现方法：

- func (p *Parser) parseEqual(out *NamedCell) error

其次对 WHERE 语句进行解析，其由关键字 WHERE、等于结构 c = 1、关键字 AND 等元素构成，可以使用 Parser.tryKeyword()、Parser.parseEqual() 和 Parser.tryPunctuation(";") 方法进行解析。

- func (p *Parser) parseWhere(out *[]NamedCell) error

最后解析 SELECT 语句，其由关键字 SELECT、列名 a, b、关键字 FROM、表名 t 以及 WHERE 语句组成，因此可以使用 Parser.tryKeyword()、Parser.tryName() 和 Parser.parseWhere 等方法进行解析。

- func (p *Parser) parseSelect(out *StmtSelect) error