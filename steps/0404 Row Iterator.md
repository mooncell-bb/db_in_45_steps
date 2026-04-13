KVIterator 能够对数据库存储的所有数据进行遍历，现在在 database/table.go 中添加 RowIterator 结构体，其是对 KVIterator 结构体的封装，用于遍历数据库中同一张表中的相关数据。

```go
type RowIterator struct {
	schema *Schema
	iter   *storage.KVIterator
	valid  bool
	row    Row
}

func (iter *RowIterator) Valid() bool {
	return iter.valid
}

func (iter *RowIterator) Row() Row {
	return iter.row
}
```

使用 schema 和 row 作为参数，寻找某张表的第一行记录，然后顺序遍历 KV 数组，直到该表的记录遍历完毕。

由于会使用 Row.DecodeKey() 方法将 KV 数组中的数据转换为 row 并检查该表是否属于给定 schema，因此需要使用一个标志来判断该表记录遍历完毕，因此在 database/row.go 中修改 Row.DecodeKey() 方法：

```go
var ErrOutOfRange = errors.New("out of range")

func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	if len(key) < len(schema.Table)+1 {
		return ErrOutOfRange
	}

	index := slices.Index(key, 0x00)
	if index == -1 {
		return errors.New("cannot find table info")
	}

	table := string(key[:index])
	if table != schema.Table {
		return ErrOutOfRange
	}

	if len(row) != len(schema.Cols) {
		panic("decode key failure")
	}

	key = key[len(schema.Table)+1:]

	for _, idx := range schema.PKey {
		col := schema.Cols[idx]
		row[idx] = Cell{Type: col.Type}

		if key, err = row[idx].DecodeKey(key); err != nil {
			return err
		}
	}

	if len(key) != 0 {
		return errors.New("trailing garbage")
	}

	return nil
}
```

通过 ErrOutOfRange 标志来确定该行记录是否属于某个表。

之后在 table.go 中添加辅助方法 decodeKVIter()，获取数据转化为 row，并检查解析的表数据是否合法：

- func decodeKVIter(schema *Schema, iter *storage.KVIterator, row Row) (bool, error)

```go
func decodeKVIter(schema *Schema, iter *storage.KVIterator, row Row) (bool, error) {
	if !iter.Valid() {
		return false, nil
	}

	if err := row.DecodeKey(schema, iter.Key()); err == ErrOutOfRange {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if err := row.DecodeVal(schema, iter.Val()); err != nil {
		return false, err
	}

	return true, nil
}
```

实现 RowIterator 的其他方法：

- func (db \*DB) Seek(schema \*Schema, row Row) (\*RowIterator, error)
- func (iter *RowIterator) Next() (err error)