database/table.go 中已经实现部分关系型数据库接口：

```go
func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error)
func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error)
func (db *DB) Select(schema *Schema, row Row) (ok bool, err error)
func (db *DB) Update(schema *Schema, row Row) (updated bool, err error)
```

现在 parser 包已经可以将 SQL 语句解析为对应的 StmtXXX 类型，因此可以将两部分结合起来。

新增 parser/sql_exec.go 文件，定义 Exec 结构体，并提供 Exec.ExecStmt() 方法，其接收任意 StmtXXX 类型，并匹配给定的类型来执行数据库相关的方法。此外，还需要额外定义一个 SQLResult 结构体，用来返回执行结果。

```
parser                  
├─ sql_exec.go          
├─ sql_exec_test.go     
├─ sql_parse.go         
├─ sql_parser_test.go   
└─ sql_parser_utils.go  
```

```go
type Exec struct {
	DB *database.DB
}

type SQLResult struct {
	Updated int
	Header  []string
	Values  []database.Row
}

func (exec *Exec) Open() error {
	return exec.DB.Open()
}

func (exec *Exec) Close() error {
	return exec.DB.KV.Close()
}

func (exec *Exec) ExecStmt(stmt any) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreatTable:
		err = exec.execCreateTable(ptr)
	case *StmtSelect:
		r.Header = ptr.cols
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

- SELECT 语句返回 Header 和 Values，之后的实现中可以返回多行数据，因此使用了切片类型。
- 其他更新语句返回 Updated，即受影响的行数，目前该值只能是 0 或 1。
- 新增 Exec 的执行方法：
  - func (exec *Exec) execCreateTable(stmt *StmtCreatTable) (err error)
  - func (exec *Exec) execDelete(stmt *StmtDelete) (count int, err error)
  - func (exec *Exec) execInsert(stmt *StmtInsert) (count int, err error)
  - func (exec *Exec) execSelect(stmt *StmtSelect) ([]database.Row, error)
  - func (exec *Exec) execUpdate(stmt *StmtUpdate) (count int, err error)

由于 DB 的数据库接口都依赖于 Schema 入参，因此首先实现 Exec.execCreateTable() 方法，其会将 StmtCreatTable 结构体解析为 Schema 结构体，并存储在数据库中。

database/table.go 的 DB 结构体新增 Tables 字段，用于存储 Schema 结构，同时新增 DB.GetSchma() 方法用于获取 Schema 结构。当从 KV 中获取到表结构时，会将其存储在此 Tables map 中，后续再次获取时可直接返回。

```go
type DB struct {
	KV     storage.KV
	Tables map[string]Schema
}

func (db *DB) Open() error {
	db.Tables = make(map[string]Schema)
	return db.KV.Open()
}

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.Tables[table]

	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))

		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}

		if err != nil {
			return Schema{}, err
		}

		if !ok {
			return Schema{}, errors.New("table is not found")
		}

		db.Tables[table] = schema
	}

	return schema, nil
}
```

规定了 Schema 在 KV 中存储的格式为：key = @schema_tableName，value = json(table)，因此 Exec.execCreateTable() 方法在存储时也需要使用此格式。

