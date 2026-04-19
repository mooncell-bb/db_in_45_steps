修改 StmtSelect 和 StmtUpdate 结构体，使其支持更复杂的表达式。

```sql
SELECT a * 4 - b, d + c FROM t WHERE d = 123;
UPDATE t SET a = a - b, b = a, c = d + c WHERE d = 123;
```

parser/sql_parser 中修改相关结构体：

```go
type StmtSelect struct {
	table string
	cols  []any // ExprUnOp | ExprBinOp | string | *Cell
	keys  []database.NamedCell
}

type StmtUpdate struct {
	table string
	keys  []database.NamedCell
	value []ExprAssign
}

type ExprAssign struct {
	column string
	expr   any // ExprUnOp | ExprBinOp | string | *Cell
}
```

新增 Parser.parseAssign() 方法，将 a = a - b 结构赋值给 ExprAssign 结构体。

- func (p *Parser) parseAssign(out *ExprAssign) (err error)

在 Parser.parseSelect() 方法中，将 tryName() 替换为 parseExpr()。

```go
func (p *Parser) parseSelect(out *StmtSelect) error {
	for !p.tryKeyword("FROM") {
		if len(out.cols) > 0 && !p.tryPunctuation(",") {
			return errors.New("expect comma")
		}

		expr, err := p.ParseExpr()
		if err != nil {
			return err
		}

		out.cols = append(out.cols, expr)
	}
    
    ...
    
}
```

在 Parser.parseUpdate() 方法中，将 parseEqual() 替换为 parseAssign()。

```go
func (p *Parser) parseUpdate(out *StmtUpdate) error {
	...

	for !p.tryKeyword("WHERE") {
		expr := ExprAssign{}

		if len(out.value) > 0 && !p.tryKeyword(",") {
			return errors.New("expect ,")
		}

		if err := p.parseAssign(&expr); err != nil {
			return err
		}

		out.value = append(out.value, expr)
	}

	...
}
```

修改 parser/sql_exec.go 中的相关方法，表达式会调用 evalExpr() 方法获取结果。

Exec.ExecStmt() 方法中，之前直接将 SELECT a, b 中的 a、b 输出，现在 StmtSelect 中的 cols 被修改为了 any，其有 string、Cell、ExprUnOp 和 ExprBinOp 四种形式，需要将其转化为 string 输出。

首先在 database/cell.go 中创建函数 CellToStr()，其将 Cell 转化为 string 输出。

- func CellToStr(cell *Cell) string

```go
func CellToStr(cell *Cell) string {
	switch cell.Type {
	case TypeI64:
		return strconv.FormatInt(cell.I64, 10)
	case TypeStr:
		return string(cell.Str)
	default:
		panic("unreachable")
	}
}
```

然后在 database/operator.go 中添加函数 ExpropToStr()，其将 ExprOp 转化为对应符号。

- func ExpropToStr(op ExprOp) string

```go
func ExpropToStr(op ExprOp) string {
	switch op {
	case OP_ADD:
		return "+"
	case OP_SUB:
		return "-"
	case OP_MUL:
		return "*"
	case OP_DIV:
		return "/"
	case OP_EQ:
		return "="
	case OP_NE:
		return "!="
	case OP_LE:
		return "<="
	case OP_GE:
		return ">="
	case OP_LT:
		return "<"
	case OP_GT:
		return ">"
	case OP_AND:
		return "AND"
	case OP_OR:
		return "OR"
	case OP_NOT:
		return "NOT"
	case OP_NEG:
		return "-"
	default:
		panic("unreachable")
	}
}
```

然后在 parser/eval.go 方法中编写 ExprTostr() 函数，其处理四种形式。

- func ExprTostr(expr any) string

```go
func ExprTostr(expr any) string {
	switch e := expr.(type) {
	case string:
		return e
	case *database.Cell:
		return database.CellToStr(e)
	case *ExprUnOp:
		switch e.op {
		case database.OP_NEG:
			return "-" + ExprTostr(e.kid)
		case database.OP_NOT:
			return "NOT " + ExprTostr(e.kid)
		default:
			panic("unreachable")
		}
	case *ExprBinOp:
		return "(" + ExprTostr(e.left) + " " + database.ExpropToStr(e.op) + " " + ExprTostr(e.right) + ")"
	default:
		panic("unreachable")
	}
}
```

最后在 parser/eval.go 方法中编写 ExprsToHeader() 函数，其循环处理 cols 并返回 []string。

- func ExprsToHeader(cols []any) (header []string)

```go
func ExprsToHeader(cols []any) (header []string) {
	for _, expr := range cols {
		header = append(header, ExprTostr(expr))
	}

	return
}
```

Exec.ExecStmt() 方法中使用转换 ExprsToHeader() 函数：

```go
func (exec *Exec) ExecStmt(stmt any) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreatTable:
		err = exec.execCreateTable(ptr)
	case *StmtSelect:
		r.Header = ExprsToHeader(ptr.cols)
		r.Values, err = exec.execSelect(ptr)
	case *StmtInsert:
		r.Updated, err = exec.execInsert(ptr)
	case *StmtUpdate:
		r.Updated, err = exec.execUpdate(ptr)
	case *StmtDelete:
		r.Updated, err = exec.execDelete(ptr)
	default:
		panic("unreachable")
	}
	return
}
```

Exec.execSelect() 方法在获取记录后，计算表达式值并转化为 Cell 结构体。

```go
func (exec *Exec) execSelect(stmt *StmtSelect) ([]database.Row, error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	row, err := database.MakePKey(&schema, stmt.keys)
	if err != nil {
		return nil, err
	}

	if ok, err := exec.DB.Select(&schema, row); err != nil || !ok {
		return nil, err
	}

	out := make(database.Row, len(stmt.cols))
	for idx, expr := range stmt.cols {
		cell, err := evalExpr(&schema, row, expr)
		if err != nil {
			return nil, err
		}

		out[idx] = *cell
	}

	return []database.Row{row}, nil
}
```

Exec.execUpdate() 方法需要计算 a = a - b 中的 a - b，因此也需要计算表达式值并转化为 Cell。

```go
func (exec *Exec) execUpdate(stmt *StmtUpdate) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	row, err := database.MakePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}

	if ok, err := exec.DB.Select(&schema, row); err != nil || !ok {
		return 0, err
	}

	updates := make([]database.NamedCell, len(stmt.value))
	for idx, assign := range stmt.value {
		cell, err := evalExpr(&schema, row, assign.expr)
		if err != nil {
			return 0, err
		}

		updates[idx] = database.NamedCell{Column: assign.column, Value: *cell}
	}

	if err = database.FillNonPKey(&schema, updates, row); err != nil {
		return 0, err
	}

	updated, err := exec.DB.Update(&schema, row)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}

	return count, nil
}
```

