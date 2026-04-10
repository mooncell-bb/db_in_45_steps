当前 SQL 只能通过主键操作单行数据，现在修改为可以范围查询数据。为了实现类似 a >= 123 的查询条件，则需要先找到 a = 123 的数据，然后顺序查询 a > 123 的其它数据。

当前 storage 查询数据的核心是一个 Mem map，该数据结构无法进行范围查询。若要返回多行记录，则需要查询 map 中的所有数据，然后依次过滤比较。storage/kv.go 中修改 KV 结构体的存储方式，使用有序数组来替代 map：

```go
type KV struct {
	Log  Log
	Keys [][]byte
	Vals [][]byte
}
```

修改 KV 结构体的相关方法，首先修改 KV.Get() 方法，由于数组已经有序，则可以使用二分搜索快速查找：

- func (kv *KV) Get(key []byte) (val []byte, ok bool, err error)

```go
func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare); ok {
		return kv.Vals[idx], true, nil
	}

	return nil, false, nil
}
```

- func (kv *KV) Del(key []byte) (deleted bool, err error)

```go
func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare); ok {
		if err := kv.Log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err
		}

		kv.Keys = slices.Delete(kv.Keys, idx, idx+1)
		kv.Vals = slices.Delete(kv.Vals, idx, idx+1)
	}

	return false, nil
}
```

- func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) 

```go
func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	idx, exist := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare)

	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(kv.Vals[idx], val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(kv.Vals[idx], val)
	default:
		panic("update mode mismatch")
	}

	if updated {
		if err = kv.Log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}

		if exist {
			kv.Vals[idx] = val
		} else {
			kv.Keys = slices.Insert(kv.Keys, idx, key)
			kv.Vals = slices.Insert(kv.Vals, idx, val)
		}
	}

	return
}
```

- func (kv *KV) Open() error

首先获取日志中的所有结构体，得到 entries 切片。对于同一个 key，其由写 Entry 结构体、删除 Entry 结构体构成，且在后的 Entry 结构体必须覆盖前面的 Entry 结构体。

slices.SortStableFunc() 函数对是一个稳定排序，不会改变同一个元素的相对位置。因此对 entries 切片排序后，同一类 Entry 结构体会合并在一起，且原始的相对位置不会改变；若存在删除删除 Entry，则也会在最后一个位置。

```go
func (kv *KV) Open() error {
	if err := kv.Log.Open(); err != nil {
		return err
	}

	entries := []Entry{}
	for {
		ent := Entry{}
		if eof, err := kv.Log.Read(&ent); err != nil {
			return err
		} else if eof {
			break
		}

		entries = append(entries, ent)
	}

	slices.SortStableFunc(entries, func(a, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})

	kv.Keys, kv.Vals = kv.Keys[:0], kv.Vals[:0]
	for _, ent := range entries {
		n := len(kv.Keys)
		if n > 0 && bytes.Equal(kv.Keys[n-1], ent.key) {
			kv.Keys, kv.Vals = kv.Keys[:n-1], kv.Vals[:n-1]
		}

		if !ent.deleted {
			kv.Keys = append(kv.Keys, ent.key)
			kv.Vals = append(kv.Vals, ent.val)
		}
	}

	return nil
}
```

