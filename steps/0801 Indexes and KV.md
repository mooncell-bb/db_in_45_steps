parser/sql_parser.go 的 StmtCreatTable 结构中添加 indices 用于保存索引：

```go
type StmtCreatTable struct {
	table   string
	cols    []database.Column
	pkey    []string
	indices [][]string
}
```

database/row.go 的 Schema 结构也需相应替换：

```go
type Schema struct {
	Table   string
	Cols    []Column
	Indices [][]int
}
```

主键本质上是一种特殊的索引，因此使用 Indices[0] 作为 PKey。

