不同索引的键必须保持唯一性，因此需添加 indexNo 参数并将其编码至键中。

- func (row Row) EncodeKey(schema *Schema, indexNo int) (key []byte)

```go
func (row Row) EncodeKey(schema *Schema, indexNo int) (key []byte) {
	if len(row) != len(schema.Cols) {
		panic("mismatch between row data and schema")
	}

	key = append([]byte(schema.Table), 0x00, byte(indexNo))

	for _, idx := range schema.Indices[indexNo] {
		cell := row[idx]
		if cell.Type != schema.Cols[idx].Type {
			panic("cell type mismatch")
		}

		key = append(key, byte(cell.Type))
		key = cell.EncodeKey(key)
	}

	return append(key, 0x00)
}
```

- func (row Row) DecodeKey(schema *Schema, indexNo int, key []byte) (err error)

```go
func (row Row) DecodeKey(schema *Schema, indexNo int, key []byte) (err error) {
	if len(row) != len(schema.Cols) {
		panic("mismatch between row data and schema")
	}

	if len(key) < len(schema.Table)+2 {
		return ErrOutOfRange
	}

	if string(key[:len(schema.Table)+2]) != schema.Table+"\x00"+string(byte(indexNo)) {
		return ErrOutOfRange
	}
	key = key[len(schema.Table)+2:]

	for _, idx := range schema.Indices[indexNo] {
		col := schema.Cols[idx]
		if len(key) == 0 {
			return ErrDataLen
		}

		if CellType(key[0]) != col.Type {
			return errors.New("cell type mismatch")
		}
		key = key[1:]

		row[idx] = Cell{Type: col.Type}

		if key, err = row[idx].DecodeKey(key); err != nil {
			return err
		}
	}

	if len(key) != 1 || key[0] != 0x00 {
		return errors.New("trailing garbage")
	}

	return nil
}
```

- func EncodeKeyPrefix(schema *Schema, indexNo int, prefix []Cell, positive bool) []byte

```go
func EncodeKeyPrefix(schema *Schema, indexNo int, prefix []Cell, positive bool) []byte {
	if len(prefix) > len(schema.Indices[0]) {
		panic("mismatch between key prefix and schema")
	}

	key := append([]byte(schema.Table), 0x00, byte(indexNo))
	for idx, cell := range prefix {
		if cell.Type != schema.Cols[schema.Indices[indexNo][idx]].Type {
			panic("cell type mismatch")
		}

		key = append(key, byte(cell.Type))
		key = cell.EncodeKey(key)
	}

	if positive {
		key = append(key, 0xff)
	}

	return key
}
```

在进行 INSERT、UPDATE 和 DELETE 操作时，需要更新或移除索引键：

1. INSERT 添加记录时，插入索引键。
2. DELETE 删除记录时，需同时删除索引键。
3. UPDATE 更新现有记录时，可能需要移除旧的索引键。