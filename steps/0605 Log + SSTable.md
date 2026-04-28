在 storage/kv.go 的 KV 结构体中，新增 SortedFile：

```go
type KV struct {
	Log  Log
	Mem  SortedArray
	Main SortedFile
}
```

修改 SortedFile.Open() 方法，首先将原有基于日志的方法修改为 openLog() 名称。

然后 storage/sorted_file.go 文件中新增 SortedFile.Open() 方法：

- func (file *SortedFile) Open() (err error)

```go
func (file *SortedFile) Open() (err error) {
	file.fp, err = os.OpenFile(file.FileName, os.O_RDONLY, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}

	if err = file.openExisting(); err != nil {
		_ = file.Close()
	}

	return err
}

func (file *SortedFile) openExisting() error {
	var buf [8]byte
	if _, err := file.fp.ReadAt(buf[:8], 0); err != nil {
		return err
	}

	file.nkeys = int(binary.LittleEndian.Uint64(buf[:8]))
	return nil
}
```

kv.go 中新增 KV.openSSTable() 方法，用于开启 SortedFile：

- func (kv *KV) openSSTable() error

```go
func (kv *KV) openSSTable() error {
	if kv.Main.FileName != "" {
		if err := kv.Main.Open(); err != nil {
			return err
		}
	}
	
	return nil
}
```

新增 openAll() 方法，调用 Log 和 SortedFile 的 Open() 方法：

- func (kv *KV) openAll() error

```go
func (kv *KV) openAll() error {
	if err := kv.openLog(); err != nil {
		return err
	}
    
	return kv.openSSTable()
}
```

最后实现原有的 KV.Open() 方法：

```go
func (kv *KV) Open() (err error) {
	if err = kv.openAll(); err != nil {
		_ = kv.Close()
	}
	
	return err
}
```

当前的 Close() 方法只关闭了 Log，现在还需要关闭 SortedFile。由于未来可能有多个 SortedFile，因此新增一个 MultiClosers 结构，用于管理所有的关闭。

storage 新增 kv_utils.go 文件，其中实现 MultiClosers 结构体：

```go 
type MultiClosers []io.Closer

func (mc *MultiClosers) Close() (reterr error) {
	for _, item := range *mc {
		if err := item.Close(); err != nil {
			reterr = err
		}
	}
	*mc = nil
	return reterr
}
```

kv.go 中新增 MultiClosers，并在 Open() 方法中添加需关闭的结构：

```go
type MultiClosers []io.Closer

func (mc *MultiClosers) Close() (reterr error) {
	for _, item := range *mc {
		if err := item.Close(); err != nil {
			reterr = err
		}
	}

	*mc = nil
	return reterr
}

func (kv *KV) openLog() error {
	if err := kv.Log.Open(); err != nil {
		return err
	}

	kv.MultiClosers = append(kv.MultiClosers, &kv.Log)

	...
}

func (kv *KV) openSSTable() error {
	if kv.Main.FileName != "" {
		if err := kv.Main.Open(); err != nil {
			return err
		}

		kv.MultiClosers = append(kv.MultiClosers, &kv.Main)
	}

	return nil
}
```

删除原有 KV.Close() 方法，则 KV.Open() 中可直接调用 MultiClosers.Close() 方法。

当前存在两级数据结构，已删除的键必须在上层记录，否则查询会从下层返回这些键。

SortedArray 中新增 deleted 标志，用于记录已删除的键：

```go
type SortedArray struct {
	Keys    [][]byte
	Vals    [][]byte
	Deleted []bool
}

func (arr *SortedArray) Iter() (SortedKVIter, error) {
	return &SortedArrayIter{arr.Keys, arr.Vals, arr.Deleted, 0}, nil
}
```

```go
type SortedArrayIter struct {
	Keys    [][]byte
	Vals    [][]byte
	deleted []bool
	pos     int
}

func (iter *SortedArrayIter) Deleted() bool {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.deleted[iter.pos]
}
```

重构 SortedArray.Del() 方法，若数组中存在元素则删除，否则添加删除记录：

- func (arr *SortedArray) Del(key []byte) (deleted bool, err error)

```go
func (arr *SortedArray) Del(key []byte) (deleted bool, err error) {
	idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)
	exist := ok && !arr.Deleted[idx]
	if exist {
		arr.Vals[idx] = nil
		arr.Deleted[idx] = true
		
		return true, nil
	} else {
		arr.Keys = slices.Insert(arr.Keys, idx, key)
		arr.Vals = slices.Insert(arr.Vals, idx, nil)
		arr.Deleted = slices.Insert(arr.Deleted, idx, true)

		return false, nil
	}
}
```

相关函数也需要修改：

```go
func (arr *SortedArray) Set(key []byte, val []byte) (updated bool, err error) {
	idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	updated = !ok || arr.Deleted[idx] || !bytes.Equal(val, arr.Vals[idx])
	if updated {
		if ok {
			arr.Vals[idx] = val
			arr.Deleted[idx] = false
		} else {
			arr.Keys = slices.Insert(arr.Keys, idx, key)
			arr.Vals = slices.Insert(arr.Vals, idx, val)
			arr.Deleted = slices.Insert(arr.Deleted, idx, false)
		}
	}

	return updated, nil
}

func (arr *SortedArray) Push(key []byte, val []byte, deleted bool) {
	arr.Keys = append(arr.Keys, key)
	arr.Vals = append(arr.Vals, val)
	arr.Deleted = append(arr.Deleted, deleted)
}

func (arr *SortedArray) Pop() {
	n := arr.Size()
	arr.Keys = arr.Keys[:n-1]
	arr.Vals = arr.Vals[:n-1]
	arr.Deleted = arr.Deleted[:n-1]
}
```

