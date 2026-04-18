新增多种 SQL 运算符，优先级从低到高依次为：

```
a OR b
a AND b
NOT a
a = b, a <> b, a < b, a > b, a <= b, a >= b
a + b, a - b
a * b, a / b
-a
```

NOT a 和 -a 仅有一个子节点，因此还需要构建一个 ExprUnOp 结构体：

```go
type ExprUnOp struct {
	op  database.ExprOp
	kid any
}
```

在 database/operator.go 中新增相关的操作符枚举值：

```go
const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_MUL ExprOp = 3  // *
	OP_DIV ExprOp = 4  // /
	OP_EQ  ExprOp = 10 // =
	OP_NE  ExprOp = 11 // !=
	OP_LE  ExprOp = 12 // <=
	OP_GE  ExprOp = 13 // >=
	OP_LT  ExprOp = 14 // <
	OP_GT  ExprOp = 15 // >
	OP_AND ExprOp = 20 // AND
	OP_OR  ExprOp = 21 // OR
	OP_NOT ExprOp = 30 // not
	OP_NEG ExprOp = 31 // -
)
```

每一个优先级都对应一个 Parser.parseXXX() 方法，该函数会调用下一个 Parser.parseYYY() 方法。之前编写的 parseAdd() 和 parseMul() 方法有相同逻辑，其也可以被用于其它层次的逻辑处理，可以被抽象出来：

- func (p *Parser) parseBinop(tokens []string, ops []database.ExprOp, inner func() (any, error)) (any, error)

```go
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
```

然后从高到低的优先级，编写对应的 Parser.parseXXX() 方法。

- func (p *Parser) parseNeg() (expr any, err error)

负数表达式可以为 a、-a、--a 等形式，因此不能只判断第一个 - 符号，可以使用递归的方式来处理。

- func (p *Parser) parseMul() (any, error)
- func (p *Parser) parseAdd() (any, error)

乘除法表达式和加减法表达式可以复用 Parser.parseBinop() 方法。

```go
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
```

- func (p *Parser) parseCmp() (any, error)

```go
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
```

- func (p *Parser) parseNot() (expr any, err error)

```go
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
```

- func (p *Parser) parseAnd() (any, error)

```go
func (p *Parser) parseAnd() (any, error) {
	tokens := []string{"AND"}
	ops := []database.ExprOp{database.OP_AND}
	return p.parseBinop(tokens, ops, p.parseNot)
}
```

- func (p *Parser) parseOr() (interface{}, error)

```go
func (p *Parser) parseOr() (any, error) {
	tokens := []string{"OR"}
	ops := []database.ExprOp{database.OP_OR}
	return p.parseBinop(tokens, ops, p.parseAnd)
}
```

- func (p *Parser) ParseExpr() (any, error)

```go
func (p *Parser) ParseExpr() (any, error) {
	return p.parseOr()
}
```

最后，在 paser/eval.go 中的 evalExpr() 函数也要处理其他的符号。

首先新增 case *ExprUnOp 的判断，当符号为 - 且 type 为 int64 时返回新的 Cell。

然后规定表达式的结果为 type = int64 类型的 Cell，若为 1 则表示 true，为 0 时表示 false。在符号为 NOT 且 type 为 int64 时，反转表达式结果。

```go
case *ExprUnOp:
    kid, err := evalExpr(schema, row, e.kid)
    if err != nil {
        return nil, err
    }

    if e.op == database.OP_NEG && kid.Type == database.TypeI64 {
        return &database.Cell{Type: database.TypeI64, I64: -kid.I64}, nil
    } else if e.op == database.OP_NOT && kid.Type == database.TypeI64 {
        res := int64(0)
        if kid.I64 == 0 {
            res = 1
        }

        return &database.Cell{Type: database.TypeI64, I64: res}, nil
    }

    return nil, errors.New("bad unary op")
```

然后补充对 case *ExprBinOp 分支的判断：

- 若操作符为 + 且类型为 string，则进行拼接。
- 若操作符为 +、-、*、/，且类型为 int，则执行对应运算。
- 若操作符为 AND、OR，且类型为 int，则执行逻辑运算。

```go
out := &database.Cell{Type: left.Type}
switch {
case e.op == database.OP_ADD && out.Type == database.TypeStr:
    out.Str = slices.Concat(left.Str, right.Str)

case e.op == database.OP_ADD && out.Type == database.TypeI64:
    out.I64 = left.I64 + right.I64
case e.op == database.OP_SUB && out.Type == database.TypeI64:
    out.I64 = left.I64 - right.I64
case e.op == database.OP_MUL && out.Type == database.TypeI64:
    out.I64 = left.I64 * right.I64
case e.op == database.OP_DIV && out.Type == database.TypeI64:
    if right.I64 == 0 {
        return nil, errors.New("division by 0")
    }
    out.I64 = left.I64 / right.I64

case e.op == database.OP_AND && out.Type == database.TypeI64:
    if left.I64 != 0 && right.I64 != 0 {
        out.I64 = 1
    }
case e.op == database.OP_OR && out.Type == database.TypeI64:
    if left.I64 != 0 || right.I64 != 0 {
        out.I64 = 1
    }
default:
    return nil, errors.New("bad binary op")
}
```

此外还需要增加对比较运算符的判断，其返回表达式结果。

```go
switch e.op {
case database.OP_EQ, database.OP_NE, database.OP_LE, database.OP_GE, database.OP_LT, database.OP_GT:
    res := 0
    switch out.Type {
    case database.TypeI64:
        res = cmp.Compare(left.I64, right.I64)
    case database.TypeStr:
        res = bytes.Compare(left.Str, right.Str)
    default:
        panic("unknown type")
    }

    b := false
    switch e.op {
    case database.OP_EQ:
        b = (res == 0)
    case database.OP_NE:
        b = (res != 0)
    case database.OP_LE:
        b = (res <= 0)
    case database.OP_GE:
        b = (res >= 0)
    case database.OP_LT:
        b = (res < 0)
    case database.OP_GT:
        b = (res > 0)
    }

    if b {
        out.I64 = 1
    }

    return out, nil
}
```

