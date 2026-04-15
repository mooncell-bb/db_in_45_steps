当前已经在 database 中添加了范围查询的功能，但解析 SQL 的 parser 部分目前只能解析固定的 WHERE：

```sql
SELECT a, b FROM t WHERE b = 1 AND c = 'x';
```

现在增强 parser 板块内容，需要支持解析以下形式：

```sql
SELECT a, b FROM t WHERE a > 123;
SELECT a, b FROM t WHERE (a, b) > (123, 0);
```

首先将 WHERE 子句映射为一个数据结构 ExprBinOp，在 parser 中定义该结构体：

```go
type ExprBinOp struct {
	op    database.ExprOp
	left  any
	right any
}
```

```
// a > 123
ExprBinOp{
	op:		OP_GT,
    left:	"a",
    right:	&Cell{Type: TypeI64, I64: 123}
}

// a = 123 and b = 456
ExprBinOp{
	op:		OP_AND,
    left:  	&ExprBinOp{op: OP_EQ, left: "a", right: &Cell{Type: TypeI64, I64: 123}},
    right: 	&ExprBinOp{op: OP_EQ, left: "b", right: &Cell{Type: TypeI64, I64: 456}},
}
```

最基本的情况是 a > 123 其中 left 的值为 string，right 的值为 Cell。

对于复杂表达式，例如 a = 123 and b = 456，其值也可以是 ExprBinOp，表示一个嵌套表达式。

首先解析加减表达式，在 database/operator 中新增加减表达式枚举值：

```go
const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_LE  ExprOp = 12 // <=
	OP_GE  ExprOp = 13 // >=
	OP_LT  ExprOp = 14 // <
	OP_GT  ExprOp = 15 // >
)
```

加减表达式的形式可能为 3、a、a + b、a + b -3，其可能返回 string、ExprBinOp：

```
// 3
&Cell{Type: TypeI64, I64: 3}

// a
"a"

// a + b
&ExprBinOp{op: OP_ADD, left: "a", right: "b"}

// a + b - 3
&ExprBinOp{
	op: 	OP_SUB,
    left:  	&ExprBinOp{op: OP_ADD, left: "a", right: "b"},
    right: 	&Cell{Type: TypeI64, I64: 123},
}
```

给定 Parser.parseAtom() 函数，该函数首先尝试解析 string，否则之后解析 Cell：

```go
func (p *Parser) parseAtom() (any, error) {
	if name, ok := p.tryName(); ok {
		return name, nil
	}

	cell := &database.Cell{}
	if err := p.parseValue(cell); err != nil {
		return nil, err
	}

	return cell, nil
}
```

实现 Parser.parseAdd() 函数，该函数通过循环调用 parseAtom() 和 tryPunctuation() 解析表达式：

- func (p *Parser) parseAdd() (any, error)