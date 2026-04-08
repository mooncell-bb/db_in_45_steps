```
CREATE TABLE t (a int64, b int64, c string, d string, PRIMARY KEY (a, b));
INSERT INTO t VALUES (1, 2, 'x', 'y');
DELETE FROM t WHERE c = 'x' AND d = 'y';
UPDATE t SET a = 1, b = 2 WHERE c = 'x' AND d = 'y';
```

parser/sql_parser.go 添加对应的 StmtCreatTable、StmtInsert、StmtUpdate 和 StmtDelete：

```go
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
	keys  []NamedCell
	value []NamedCell
}

type StmtDelete struct {
	table string
	keys  []NamedCell
}
```

给定 Parser.parseStmt() 方法，通过 SQL 语句的第一个关键字来判断调用哪个方法来解析语句：

```go
func (p *Parser) parseStmt() (out any, err error) {
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
```

- 修改 Parser.tryKeyword() 方法，使其能够支持多个关键词匹配。
- 修改 Parser.parseSelect() 方法，由于已经匹配了关键字 SELECT，因此该方法不需要额外再匹配关键字。
- 新增 Parser.parseCreateTable()、Parser.parseInsert() 等方法：
  - func (p *Parser) parseCreateTable(out *StmtCreatTable) error
  - func (p *Parser) parseInsert(out *StmtInsert) error
  - func (p *Parser) parseUpdate(out *StmtUpdate) error
  - func (p *Parser) parseDelete(out *StmtDelete) error

首先实现 Parser.parseUpdate() 和 Parser.parseDelete() 方法，其解析过程可以参考 SELECT 语句。

对于 INSERT 和 UPDATE 语句，其都存在 (xxx, yyy) 结构，因此首先定义 Parser.parseCommaList() 方法。该方法接收一个 func 函数来解析其中的结构，例如可以解析 ('x', 'y')，也可以解析 (a int64, b int64)。

- func (p *Parser) parseCommaList(item func() error) error

首先实现 INSERT 语句解析 (1, 2, 'x', 'y') 结构的 item 方法 Parser.parseValueItem()，其需要解析数值或者字符串，因此可以复用 Parser.parseValue() 方法。

- func (p *Parser) parseValueItem(out *[]database.Cell) error

然后实现 Parser.parseInsert() 方法，该方法在解析 (1, 2, 'x', 'y') 结构时，会调用 Parser.parseCommaList() 方法并传入 Parser.parseValueItem() 方法来解析内容。

- func (p *Parser) parseInsert(out *StmtInsert) error

```go
func (p *Parser) parseInsert(out *StmtInsert) error {
    ...
    
    err := p.parseCommaList(
		func() error {
			return p.parseValueItem()
		},
	)
    
    ...
}
```

然后实现 CREATE 语句解析 (a int64, b int64, c string, d string, PRIMARY KEY (a, b)) 结构的 item 方法 Parser.parseCreateTableItem()。

注意到此结构中存在一个 "PRIMARY KEY (a, b)"，该结构是由关键字 PRIMARY KEY 和一个 (xxx, yyy) 结构构成的，因此可以首先定义一个 Parser.parseNameItem() 方法来解析其中的 (a, b) 结构。

- func (p *Parser) parseNameItem(out *[]string) error

之后再实现 Parser.parseCreateTableItem() 方法，其会首先尝试匹配关键字 PRIMARY KEY，若匹配成功则使用 Parser.parseCommaList() 方法进行解析，否则解析 a int64 结构。

- func (p *Parser) parseCreateTableItem(out *StmtCreatTable) error

```go
func (p *Parser) parseCreateTableItem(out *StmtCreatTable) error {
	if p.tryKeyword("PRIMARY", "KEY") {
		return p.parseCommaList(
			func() error {
				return p.parseNameItem(&out.pkey)
			},
		)
	}
	
    ...
}
```

最后实现 Parser.parseCreateTable() 方法，类似于 Parser.parseInsert() 方法的实现过程。

- func (p *Parser) parseCreateTable(out *StmtCreatTable) error
