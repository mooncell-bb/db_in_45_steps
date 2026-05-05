在索引机制下，单条表记录可能涉及多个键。因此，需要事务功能来确保跨多个键操作的原子性。

```go
tx = kv.NewTX()
tx.Set("k1", "v1")
tx.Set("k2", "v2")
tx.Commit()
```

目前，日志加校验和确保了单键原子性。如果日志记录包含多个键，则就能实现事务原子性。

storage/kv.go 中添加 KVTX 结构体：

```go
type KVTX struct {
	target  *KV
	updates SortedArray
	levels  MergedSortedKV
}
```

updates 用于将数据保存到内存数组中，levels 用于在事务内读取数据。

实现 KV.NewTX() 方法，事务中的读取必须能看到自身的更新状态，其仅仅是 LSM 树的一个额外层级：

- func (kv *KV) NewTX() *KVTX

```go
func (kv *KV) NewTX() *KVTX {
	tx := &KVTX{target: kv}
	tx.levels = MergedSortedKV{&tx.updates, &kv.Mem}

	for i := range kv.Main {
		tx.levels = append(tx.levels, &kv.Main[i])
	}

	return tx
}
```

然后实现 KVTX.Seek()、KVTX.Get() 方法，从 levels 寻找指定的键并返回 SortedKVIter 迭代器。

- func (tx *KVTX) Seek(key []byte) (SortedKVIter, error)

```go
func (tx *KVTX) Seek(key []byte) (SortedKVIter, error) {
	iter, err := tx.levels.Seek(key)
	if err != nil {
		return nil, err
	}

	return FilterDeleted(iter)
}
```

- func (tx *KVTX) Get(key []byte) (val []byte, ok bool, err error)

```go
func (tx *KVTX) Get(key []byte) (val []byte, ok bool, err error) {
	iter, err := tx.Seek(key)
	ok = err == nil && iter.Valid() && bytes.Equal(iter.Key(), key)

	if ok {
		val = iter.Val()
	}
	
	return val, ok, err
}
```

- func (tx \*KVTX) Range(start, stop []byte, desc bool) (\*RangedKVIter, error)

```go
func (tx *KVTX) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	iter, err := tx.Seek(start)
	if err != nil {
		return nil, err
	}

	if desc && (!iter.Valid() || bytes.Compare(iter.Key(), start) > 0) {
		if err = iter.Prev(); err != nil {
			return nil, err
		}
	}

	return &RangedKVIter{iter: iter, stop: stop, desc: desc}, nil
}
```

利用 KVTX.Get() 方法来实现 KVTX.SetEx() 方法，同时推迟日志写入。

- func (tx *KVTX) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error)
- func (tx *KVTX) Set(key []byte, val []byte) (updated bool, err error)

```go
func (tx *KVTX) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	oldVal, exist, err := tx.Get(key)
	if err != nil {
		return false, err
	}

	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(oldVal, val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(oldVal, val)
	default:
		panic("unreachable")
	}

	if updated {
		_, err = tx.updates.Set(key, val)
	}

	return
}
```

```go
func (tx *KVTX) Set(key []byte, val []byte) (updated bool, err error) {
	return tx.SetEx(key, val, ModeUpsert)
}
```

实现 KVTX.Del() 方法。

```go
func (tx *KVTX) Del(key []byte) (deleted bool, err error) {
	if _, exist, err := tx.Get(key); err != nil || !exist {
		return false, err
	}

	return tx.updates.Del(key)
}
```

实现 KVTX.Commit() 方法，提交事务将整体信息写入 Log，同时更新 Mem。

- func (tx *KVTX) Commit() error
- func (kv *KV) applyTX(tx *KVTX) error

```go
func (tx *KVTX) Commit() error {
	return tx.target.applyTX(tx)
}
```

```go
func (kv *KV) applyTX(tx *KVTX) error {
	if err := kv.updateLog(tx); err != nil {
		return err
	}

	kv.updateMem(tx)
	return nil
}
```

KV.updateLog() 方法用于将所有 updates 记录写入日志，KV.updateMem() 方法用于更新 KV.mem。

- func (kv *KV) updateLog(tx *KVTX) error

```go
func (kv *KV) updateLog(tx *KVTX) error {
	iter, err := tx.updates.Iter()
	for ; err == nil && iter.Valid(); err = iter.Next() {
		err = kv.Log.Write(
			&Entry{
				key:     iter.Key(),
				val:     iter.Val(),
				deleted: iter.Deleted(),
			},
		)

		if err != nil {
			return err
		}
	}

	return nil
}
```

- func (kv *KV) updateMem(tx *KVTX)

```go
func (kv *KV) updateMem(tx *KVTX) {
	iter, err := tx.updates.Iter()
	for ; err == nil && iter.Valid(); err = iter.Next() {
		if iter.Deleted() {
			_, err = kv.Mem.Del(iter.Key())
		} else {
			_, err = kv.Mem.Set(iter.Key(), iter.Val())
		}
	}

	if err != nil {
		panic("TX iter error")
	}
}
```

一个事务还应包含回滚方法以丢弃事务，但目前该方法不执行任何操作。

```go
func (tx *KVTX) Abort() {}
```

然后对原有的 KV 结构体 Get()、SetEx() 等方法进行修改，可使用 KVTX 进行封装。

- func (kv *KV) Get(key []byte) (val []byte, ok bool, err error)

```go
func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	tx := kv.NewTX()
	defer tx.Abort()

	return tx.Get(key)
}
```

- func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error)

```go
func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	tx := kv.NewTX()
	updated, err = tx.SetEx(key, val, mode)

	return abortOrCommit(tx, updated, err)
}
```

- func (kv *KV) Del(key []byte) (deleted bool, err error)

```go
func (kv *KV) Del(key []byte) (deleted bool, err error) {
	tx := kv.NewTX()
	deleted, err = tx.Del(key)

	return abortOrCommit(tx, deleted, err)
}
```

删除 KV.Seek()、KV.Range() 方法。

最后在 table.go 中新增 DBTX 结构体，作为对 KVTX 的简单封装。

```go
type DB struct {
	KV storage.KV
}

type DBTX struct {
	kv     *storage.KVTX
	tables map[string]Schema
}
```

- func (db *DB) NewTX() *DBTX

```go
func (db *DB) NewTX() *DBTX {
	return &DBTX{kv: db.KV.NewTX(), tables: map[string]Schema{}}
}
```

- func (tx *DBTX) Select(schema *Schema, row Row) (ok bool, err error)
- func (db *DB) Select(schema *Schema, row Row) (ok bool, err error)

```go
func (tx *DBTX) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema, 0)
	val, ok, err := tx.kv.Get(key)
	if err != nil || !ok {
		return ok, err
	}

	if err = row.DecodeVal(schema, val); err != nil {
		return false, err
	}
	
	return true, nil
}
```

```go
func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	tx := db.NewTX()
	defer tx.Abort()
	
	return tx.Select(schema, row)
}
```

- func (tx *DBTX) update(schema *Schema, row Row, mode storage.UpdateMode) (updated bool, err error)
- func (tx *DBTX) Insert(schema *Schema, row Row)
- func (tx *DBTX) Upsert(schema *Schema, row Row) (updated bool, err error)
- func (tx *DBTX) Update(schema *Schema, row Row) (updated bool, err error)
- func (tx *DBTX) Delete(schema *Schema, row Row) (deleted bool, err error)

```go
func (tx *DBTX) update(schema *Schema, row Row, mode storage.UpdateMode) (updated bool, err error) {
	key := row.EncodeKey(schema, 0)
	val := row.EncodeVal(schema)
	oldVal, exist, err := tx.kv.Get(key)
	if err != nil {
		return false, err
	}

	switch mode {
	case storage.ModeUpsert:
		updated = !exist || !bytes.Equal(oldVal, val)
	case storage.ModeInsert:
		updated = !exist
	case storage.ModeUpdate:
		updated = exist && !bytes.Equal(oldVal, val)
	default:
		panic("unreachable")
	}
	if !updated {
		return false, nil
	}

	if exist {
		oldRow := row.CopyRow()
		if err = oldRow.DecodeVal(schema, oldVal); err != nil {
			return false, err
		}
		if _, err = tx.Delete(schema, oldRow); err != nil {
			return false, err
		}
	}

	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		if i > 0 {
			key, val = row.EncodeKey(schema, i), nil
		}
		updated, err = tx.kv.SetEx(key, val, storage.ModeInsert)
		if err == nil && !updated {
			panic("impossible")
		}
	}

	return updated, err
}

func (tx *DBTX) Insert(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeInsert)
}

func (tx *DBTX) Upsert(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeUpsert)
}

func (tx *DBTX) Update(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeUpdate)
}

func (tx *DBTX) Delete(schema *Schema, row Row) (deleted bool, err error) {
	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		key := row.EncodeKey(schema, i)
		deleted, err = tx.kv.Del(key)
		if err == nil && !deleted {
			if i != 0 {
				return false, errors.New("inconsistent index")
			}
			break
		}
	}
	return deleted, err
}
```



