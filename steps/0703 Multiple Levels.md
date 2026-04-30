原来 KV 中 SortedFile 只存在一个，现在新增多个 SortedFile 层级：

```go
type KV struct {
	Options KVOptions
	Meta    KVMetaStore
	Version uint64
	Log     Log
	Mem     SortedArray
	Main    []SortedFile
	MultiClosers
}
```

KVMetaData 中也必须新增 []string 来管理多个文件：

```go
type KVMetaData struct {
	Version uint64
	SSTable []string
}
```

KV.Seek() 方法中，查询操作原本仅涉及两个层级，现在需进行调整来查询多个层级：

- func (kv *KV) Seek(key []byte) (SortedKVIter, error)

```go
func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	levels := MergedSortedKV{&kv.Mem}
	for i := range kv.Main {
		levels = append(levels, &kv.Main[i])
	}

	iter, err := levels.Seek(key)
	if err != nil {
		return nil, err
	}

	return FilterDeleted(iter)
}
```

为保证数据压缩时，已删除的数据被完全抹除，需在最末层级的 SSTable 记录已删除的键值。

```
[ key length | val length | deleted | key data | val data ]
    4bytes       4bytes      1byte
```

- func (file *SortedFile) writeSortedFile(kv SortedKV) (err error)

```go
func (file *SortedFile) writeSortedFile(kv SortedKV) (err error) {
	var buf [9]byte
	nkeys := 0
	offset := 8 + 8*kv.EstimatedSize()
	iter, err := kv.Iter()

	for ; err == nil && iter.Valid(); err = iter.Next() {
		key, val := iter.Key(), iter.Val()

		binary.LittleEndian.PutUint64(buf[:8], uint64(offset))
		if _, err = file.fp.WriteAt(buf[:8], int64(8+8*nkeys)); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
		if iter.Deleted() {
			buf[8] = 1
		} else {
			buf[8] = 0
		}
		if _, err = file.fp.WriteAt(buf[:9], int64(offset)); err != nil {
			return err
		}

		offset += 9
		if _, err = file.fp.WriteAt(key, int64(offset)); err != nil {
			return err
		}

		offset += len(key)
		if _, err = file.fp.WriteAt(val, int64(offset)); err != nil {
			return err
		}

		offset += len(val)
		nkeys++
	}

	if err != nil {
		return err
	}

	if nkeys > kv.EstimatedSize() {
		panic("SortedFile Error")
	}
	file.nkeys = nkeys

	binary.LittleEndian.PutUint64(buf[:8], uint64(nkeys))
	if _, err = file.fp.WriteAt(buf[:8], 0); err != nil {
		return err
	}

	return file.fp.Sync()
}
```

```go
type SortedFileIter struct {
	file    *SortedFile
	pos     int
	key     []byte
	val     []byte
	deleted bool
}
```

- func (file *SortedFile) index(pos int) (key []byte, val []byte, deleted bool, err error)

```go
func (file *SortedFile) index(pos int) (key []byte, val []byte, deleted bool, err error) {
	if pos < 0 || pos >= file.nkeys {
		panic("SortedFile pos Error")
	}

	var buf [9]byte
	if _, err = file.fp.ReadAt(buf[:], int64(8+8*pos)); err != nil {
		return nil, nil, false, err
	}

	offset := int64(binary.LittleEndian.Uint64(buf[:8]))
	if int64(8+8*file.nkeys) > offset {
		return nil, nil, false, errors.New("corrupted file")
	}

	if _, err = file.fp.ReadAt(buf[:9], offset); err != nil {
		return nil, nil, false, err
	}

	klen := binary.LittleEndian.Uint32(buf[0:4])
	vlen := binary.LittleEndian.Uint32(buf[4:8])
	data := make([]byte, klen+vlen)
	if _, err = file.fp.ReadAt(data, offset+9); err != nil {
		return nil, nil, false, err
	}

	deleted = buf[8] != 0
	return data[:klen], data[klen:], deleted, nil
}
```

- func (iter *SortedFileIter) loadCurrent() (err error)

```go
func (iter *SortedFileIter) loadCurrent() (err error) {
	if iter.Valid() {
		iter.key, iter.val, iter.deleted, err = iter.file.index(iter.pos)
	}

	return err
}
```

在 KV.Compact() 方法中，可直接和并 Mem，之后再进行多层和并。

- func (kv *KV) Compact() error

```go
func (kv *KV) Compact() error {
	kv.Version++
	sstable := fmt.Sprintf("sstable_%d", kv.Version)
	filename := path.Join(kv.Options.Dirpath, sstable)

	file := SortedFile{FileName: filename}
	if err := file.CreateFromSorted(&kv.Mem); err != nil {
		_ = os.Remove(filename)
		return err
	}

	meta := kv.Meta.Get()
	meta.Version = kv.Version
	meta.SSTables = slices.Insert(meta.SSTables, 0, sstable)
	if err := kv.Meta.Set(meta); err != nil {
		_ = file.Close()
		return err
	}

	kv.Main = slices.Insert(kv.Main, 0, file)
	kv.Mem.Clear()
	return kv.Log.Truncate()
}
```

- func (kv *KV) openSSTable() error

```go
func (kv *KV) openSSTable() error {
	meta := kv.Meta.Get()
	kv.Version = meta.Version
	kv.Main = kv.Main[:0]

	for _, sstable := range meta.SSTables {
		sstable = path.Join(kv.Options.Dirpath, sstable)
		file := SortedFile{FileName: sstable}
		if err := file.Open(); err != nil {
			return err
		}
		kv.MultiClosers = append(kv.MultiClosers, &file)
		kv.Main = append(kv.Main, file)
	}

	return nil
}
```

