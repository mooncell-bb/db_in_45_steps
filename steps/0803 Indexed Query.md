database/operator.go 的 RangeReq 和 table.go 的 RowIterator 结构体中，新增 IndexNo 参数，以支持索引范围查询。

```go
type RangeReq struct {
	StartCmp ExprOp
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
	IndexNo  int
}
```

```go
type RowIterator struct {
	db      *DB
	schema  *Schema
	indexNo int
	iter    *storage.RangedKVIter
	valid   bool
	row     Row
}
```

修改 decodeKVIter() 函数，以支持按索引迭代：

1. indexNo = 0，键为主键，与之前相同。
2. indexNo > 0，表示为索引，提取主键并通过 DB.Select() 方法找到行。

```go
func (iter *RowIterator) decodeKVIter() (bool, error) {
	if !iter.iter.Valid() {
		return false, nil
	}

	if err := iter.row.DecodeKey(iter.schema, iter.indexNo, iter.iter.Key()); err != nil {
		if err == ErrOutOfRange {
			panic("iter out of range err")
		}
		return false, err
	}

	if iter.indexNo > 0 {
		ok, err := iter.db.Select(iter.schema, iter.row)
		if err != nil {
			return false, err
		} else if !ok {
			return false, errors.New("inconsistent index")
		}
	} else {
		if err := iter.row.DecodeVal(iter.schema, iter.iter.Val()); err != nil {
			return false, err
		}
	}
	
	return true, nil
}
```

DB.Range()、RowIterator.Next() 方法也需相应修改。

修改 parser/sql_exec_utils.go 的 MatchRange() 函数，其会尝试每个索引并输出一个 RangeReq：

- func MatchRange(schema \*database.Schema, cond any) (\*database.RangeReq, bool)

```go
func MatchRange(schema *database.Schema, cond any) (*database.RangeReq, bool) {
	for indexNo := range schema.Indices {
		if req, ok := MatchRangeByIndex(schema, indexNo, cond); ok {
			return req, ok
		}
	}

	return nil, false
}
```

MatchRangeByIndex() 函数就是之前的 MatchRange() 函数实现。