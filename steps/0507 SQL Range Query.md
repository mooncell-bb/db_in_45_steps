为 SQL 添加范围查询功能：

```sql
SELECT a, b FROM t WHERE a > 123;
SELECT a, b FROM t WHERE (a, b) > (123, 0);
```

实现的核心机制就是将 WHERE 条件连接到现有的 RangeReq 结构体上：

```go
type RangeReq struct {
	StartCmp ExprOp
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
}
```

首先需要支持类似 (a, b) < (1, 2) 的语法，需要添加结构体并更新括号的处理逻辑。

parser/sql_parser.go 中添加 ExprTuple 结构体，用于将 (a, b) 解析为切片类型。 

```go
type ExprTuple struct {
	kids []any
}
```

添加 Parser.parseTuple() 方法，其中解析括号过程可复用 Parser.parseCommaList() 方法，解析函数可以使用 Parser.ParseExpr() 方法，若括号内只有一个表达式则直接返回，否则返回 ExprTuple 结构。

- func (p *Parser) parseTuple() (expr any, err error)

```go
func (p *Parser) parseTuple() (expr any, err error) {
	kids := []any{}
	err = p.parseCommaList(func() error {
		expr, err := p.ParseExpr()
		if err != nil {
			return err
		}

		kids = append(kids, expr)

		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(kids) == 0 {
		return nil, errors.New("empty tuple")
	}

	if len(kids) == 1 {
		return kids[0], nil
	}

	return &ExprTuple{kids}, nil
}
```

然后修改 Parser.parserAtom() 方法，其在遇到括号时不直接调用 Parser.ParseExpr() 方法进行解析，而是修改为使用刚才的 Parser.parseTuple() 方法。

```go
func (p *Parser) parseAtom() (expr any, err error) {
	if p.tryPunctuation("(") {
		p.pos--
		return p.parseTuple()
	}

	...
}
```

然后在 parser/sql_exec.go 中添加 Exec.execCond() 方法，其用于处理 WHERE 子句，并返回 RowIterator 结构体，该结构体用于范围遍历。

- func (exec \*Exec) execCond(schema \*database.Schema, cond any) (\*database.RowIterator, error)

首先不实现该方法，而是定义该方法并修改 execSelect()、execUpdate() 和 execDelete() 方法。

- func (exec *Exec) execDelete(stmt *StmtDelete) (count int, err error)

注意不能在遍历时删除元素，否则迭代器会发生错误，应该收集要删除的 key，然后遍历删除。

```go
func (exec *Exec) execDelete(stmt *StmtDelete) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	deletes := make([]database.Row, 0)
	for ; err == nil && iter.Valid(); err = iter.Next() {
		deletes = append(deletes, iter.Row().CopyRow())
	}

	for _, row := range deletes {
		updated, err := exec.DB.Delete(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}

	if err != nil {
		return 0, err
	}

	return count, nil
}
```

- func (exec *Exec) execUpdate(stmt *StmtUpdate) (count int, err error)

```go
func (exec *Exec) execUpdate(stmt *StmtUpdate) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()

		updates := make([]database.NamedCell, len(stmt.value))
		for i, assign := range stmt.value {
			cell, err := evalExpr(&schema, row, assign.expr)
			if err != nil {
				return 0, nil
			}

			updates[i] = database.NamedCell{Column: assign.column, Value: *cell}
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
	}

	if err != nil {
		return 0, err
	}

	return count, nil
}
```

- func (exec *Exec) execSelect(stmt *StmtSelect) (output []database.Row, err error)

```go
func (exec *Exec) execSelect(stmt *StmtSelect) (output []database.Row, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return nil, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()
		
		computed := make(database.Row, len(stmt.cols))
		for i, expr := range stmt.cols {
			cell, err := evalExpr(&schema, row, expr)
			if err != nil {
				return nil, err
			}

			computed[i] = *cell
		}

		output = append(output, computed)
	}

	if err != nil {
		return nil, err
	}

	return output, nil
}
```

然后实现 Exec.execCond() 方法，可以首先定义一个 MakeRange() 函数，提供 schema 和 cond，返回 RangeReq 结构体，并用该结构体来返回 RowIterator。

```go
func (exec *Exec) execCond(schema *database.Schema, cond any) (*database.RowIterator, error) {
	req, err := MakeRange(schema, cond)
	if err != nil {
		return nil, err
	}

	return exec.DB.Range(schema, req)
}
```

- func MakeRange(schema \*database.Schema, cond any) (\*database.RangeReq, error)

MakeRange() 函数检测范围查询，若能够直接解析原本的 a = 1 AND b = 2 结构，则可以直接返回快捷路径。首先在 database/row_utils.go 中实现 ExtractPKey() 函数，用于判断 NamedCell 中是否仅只包含主键 key。

```go
func ExtractPKey(schema *Schema, pkey []NamedCell) (cells []Cell, ok bool) {
	if len(schema.PKey) != len(pkey) {
		return nil, false
	}

	for _, idx := range schema.PKey {
		col := schema.Cols[idx]

		i := slices.IndexFunc(pkey, func(e NamedCell) bool {
			return col.Name == e.Column && col.Type == e.Value.Type
		})

		if i < 0 {
			return nil, false
		}

		cells = append(cells, pkey[i].Value)
	}

	return cells, true
}
```

然后使用该方法和之前的 MatchAllEq() 方法，若 WHERE 子句包含完整主键，则可以走快捷路径：

```go
func MakeRange(schema *database.Schema, cond any) (*database.RangeReq, error) {
	if keys, ok := MatchAllEq(cond, nil); ok {
		if pkey, ok := database.ExtractPKey(schema, keys); ok {
			return &database.RangeReq{
				StartCmp: database.OP_GE,
				StopCmp:  database.OP_LE,
				Start:    pkey,
				Stop:     pkey,
			}, nil
		}
	}

	if req, ok := MatchRange(schema, cond); ok {
		return req, nil
	}

	return nil, errors.New("unimplemented WHERE")
}
```

若不能完整匹配主键，则进入正常的流程，使用 MatchRange() 方法返回 RangeReq。

- func MatchRange(schema \*database.Schema, cond any) (\*database.RangeReq, bool)

匹配表达式本质上是一组条件判断语句，这里仅支持单一范围查询，例如 a > 1 或 a > 1 AND a < 2，若要支持 OR 查询，则需要映射至多次 DB.Range(schema, req)，且最佳实践是简化范围区间的并集与交集处理。

因此，WHERE 语句仅支持 a > 1 或 a > 1 AND a < 2 这两种形式：

```go
func matchRange(schema *Schema, cond interface{}) (*RangeReq, bool) {
    binop, ok := cond.(*ExprBinOp)
    if ok && binop.op == OP_AND {
        // a > 1 AND a < 2
    } else if ok {
        // a > 1
    }
    return nil, false
}
```

首先实现 MatchCmp() 函数，其将 WHERE 的 a > 1 形式进行解析并返回操作符、列名切片和实际数据。

- func MatchCmp(cond any) (database.ExprOp, []string, []database.Cell, bool)

为了实现该函数，可添加一个辅助的 AsNameList() 函数来返回列名切片，若其值是 string 直接返回，若是新增的 ExprTuple 类型，则进行训练遍历。

- func AsNameList(expr any) (out []string, ok bool)

```go
func AsNameList(expr any) (out []string, ok bool) {
	switch e := expr.(type) {
	case string:
		return []string{e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(string); ok {
				out = append(out, s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	
	return nil, false
}
```

还需要添加额外辅助函数 AsCellList()，其将表达式解析为 Cell 切片。

- func AsCellList(expr any) (out []database.Cell, ok bool)

```go
func AsCellList(expr any) (out []database.Cell, ok bool) {
	switch e := expr.(type) {
	case *database.Cell:
		return []database.Cell{*e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(*database.Cell); ok {
				out = append(out, *s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	
	return nil, false
}
```

最后实现 MatchCmp() 函数，由于表达式可写为 a > 1 也可以写为 1 < a，因此需要两次判断比较。

```go
func MatchCmp(cond any) (database.ExprOp, []string, []database.Cell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok {
		return 0, nil, nil, false
	}

	switch binop.op {
	case database.OP_LE, database.OP_GE, database.OP_LT, database.OP_GT:
	default:
		return 0, nil, nil, false
	}

	op := binop.op
	left, right := binop.left, binop.right

	names, ok := AsNameList(left)
	if !ok {
		left, right = right, left
		names, ok = AsNameList(left)
		switch op {
		case database.OP_LE:
			op = database.OP_GE
		case database.OP_GE:
			op = database.OP_LE
		case database.OP_LT:
			op = database.OP_GT
		case database.OP_GT:
			op = database.OP_LT
		}
	}

	if !ok {
		return 0, nil, nil, false
	}

	cells, ok := AsCellList(right)
	if !ok {
		return 0, nil, nil, false
	}

	return op, names, cells, true
}
```

将 a > 1 或 (a, b) > (1, 2) 的表达式转化为 cols 和 cells 后，还需要判断其是否按照主键顺序进行排列，因此在 database/row_utils.go 中编写辅助函数 IsPKeyPrefix()，用于检测排序。

- func IsPKeyPrefix(schema *Schema, cols []string, cells []Cell) bool

```go
func IsPKeyPrefix(schema *Schema, cols []string, cells []Cell) bool {
	if len(cols) != len(cells) || len(cols) > len(schema.Cols) {
		return false
	}

	for i := range cols {
		col := schema.Cols[schema.PKey[i]]

		if col.Name != cols[i] || col.Type != cells[i].Type {
			return false
		}
	}

	return true
}
```

实现 MatchRange() 函数，返回 RangeReq 结构体：

```go
func MatchRange(schema *database.Schema, cond any) (*database.RangeReq, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == database.OP_AND {
		op1, cols1, cells1, ok := MatchCmp(binop.left)
		if !ok || !database.IsPKeyPrefix(schema, cols1, cells1) {
			return nil, false
		}

		op2, cols2, cells2, ok := MatchCmp(binop.right)
		if !ok || !database.IsPKeyPrefix(schema, cols2, cells2) {
			return nil, false
		}

		if database.IsDescending(op1) != database.IsDescending(op2) {
			return nil, false
		}

		if database.IsDescending(op1) {
			op1, op2, cells1, cells2 = op2, op1, cells2, cells1
		}

		return &database.RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     cells2,
		}, true
	} else if ok {
		op1, cols1, cells1, ok := MatchCmp(cond)
		if !ok || !database.IsPKeyPrefix(schema, cols1, cells1) {
			return nil, false
		}

		op2 := database.OP_LE
		if database.IsDescending(op1) {
			op2 = database.OP_GE
		}

		return &database.RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     nil,
		}, true
	}

	return nil, false
}
```

