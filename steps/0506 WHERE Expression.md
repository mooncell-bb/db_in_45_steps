将 WHERE 部分的条件重构为一个表达式：

```go
type StmtSelect struct {
	table string
	cols  []any
	cond  any
}

type StmtUpdate struct {
	table string
	cond  any
	value []ExprAssign
}

type StmtDelete struct {
	table string
	cond  any
}
```

然后更新 Parser.parseWhere() 方法：

```go
func (p *Parser) parseWhere() (expr any, err error) {
	if !p.tryKeyword("WHERE") {
		return nil, errors.New("expect keyword")
	}

	if expr, err = p.ParseExpr(); err != nil {
		return nil, err
	}

	if !p.tryPunctuation(";") {
		return nil, errors.New("expect ;")
	}

	return expr, nil
}
```

修改 Parser.parseSelect() 方法： 

```go
func (p *Parser) parseSelect(out *StmtSelect) (err error) {
	...

	out.cond, err = p.parseWhere()
	return err
}
```

修改 Parser.parseDelete() 方法：

```go
func (p *Parser) parseDelete(out *StmtDelete) (err error) {
	if name, ok := p.tryName(); !ok {
		return errors.New("expect table name")
	} else {
		out.table = name
	}

	out.cond, err = p.parseWhere()
	return err
}
```

修改 Parser.parseUpdate() 方法：

```go
func (p *Parser) parseUpdate(out *StmtUpdate) (err error) {
	...

	p.pos -= len("WHERE")
	out.cond, err = p.parseWhere()
	return err
}
```

然后在 parser/sql_exec.go 中，执行 SELECT、UPDATE 和 DELETE 方法时，需要将 WHERE 子句中的条件转换为 Row 进行查询，现在使用一个新的 MatchPKey() 函数替代原有的 MakePKey() 函数。

parser 新增 sql_exec_utils.go 文件，添加 MakePKey() 函数：

```
parser                  
├─ eval.go              
├─ eval_test.go         
├─ sql_exec.go          
├─ sql_exec_test.go     
├─ sql_exec_utils.go    
├─ sql_parser.go        
├─ sql_parser_test.go   
└─ sql_parser_utils.go  
```

- func MatchPKey(schema *database.Schema, cond interface{}) (database.Row, error)

```go
func MatchPKey(schema *database.Schema, cond any) (database.Row, error) {
	if keys, ok := MatchAllEq(cond, nil); ok {
		return database.MakePKey(schema, keys)
	}
	
	return nil, errors.New("unimplemented WHERE")
}
```

当前 WHERE 子句仅支持 "a = 123 AND b = 456 AND ..." 形式的表达式，因此首先创建一个 MatchAllEq() 方法用于匹配该形式，之后进行拓展。

- func MatchAllEq(cond any, out []database.NamedCell) ([]database.NamedCell, bool)