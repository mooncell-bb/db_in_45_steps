当前 KV 中仅持有内存数组，但是此形式不利于后续增加磁盘数组的拓展。

storage/kv.go 中修改 KV 结构：

```go
type KV struct {
	Log Log
	Mem SortedArray
}
```

新增 storage/sorted_array.go 文件，其中保存原来的内存数组：

```go
type SortedArray struct {
	keys [][]byte
	vals [][]byte
}
```

SortedArray 需要实现 SortedArray 接口，以作为 SortedFile.CreateFromSorted() 的输入源。同时，其自身也需要携带一个 SortedArrayIter 迭代器用于遍历数据。

```go
type SortedArray struct {
	Keys [][]byte
	Vals [][]byte
}

func (arr *SortedArray) Size() int {
	return len(arr.Keys)
}

func (arr *SortedArray) Iter() (SortedKVIter, error) {
	return &SortedArrayIter{arr.Keys, arr.Vals, 0}, nil
}

type SortedArrayIter struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

func (arr *SortedArray) Seek(key []byte) (SortedKVIter, error) {
	pos, _ := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	return &SortedArrayIter{keys: arr.Keys, vals: arr.Vals, pos: pos}, nil
}

func (iter *SortedArrayIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

func (iter *SortedArrayIter) Key() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.keys[iter.pos]
}

func (iter *SortedArrayIter) Val() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.vals[iter.pos]
}

func (iter *SortedArrayIter) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos++
	}

	return nil
}

func (iter *SortedArrayIter) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	
	return nil
}
```

此外，为 SortedArray 添加辅助方法：

- func (arr *SortedArray) Clear()
- func (arr *SortedArray) Push(key []byte, val []byte)
- func (arr *SortedArray) Pop()
- func (arr *SortedArray) Key(i int) []byte
- func (arr *SortedArray) Get(key []byte) (val []byte, ok bool, err error)
- func (arr *SortedArray) Set(key []byte, val []byte) (updated bool, err error)
- func (arr *SortedArray) Del(key []byte) (deleted bool, err error)

然后 KV 的 Seek() 方法，返回通用的 SortedKVIter 接口，同时使用 Mem 的 Seek() 方法：

- func (kv *KV) Seek(key []byte) (SortedKVIter, error)

```go
func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	return kv.Mem.Seek(key)
}
```

由于 SortedArray 已经有 SortedArrayIter 迭代器了，因此原来的 KVIterator 可以删除了。

之后，基于 KVIterator 而构建的 RangedKVIter 可以使用 SortedKVIter 进行替代：

```go
type RangedKVIter struct {
	iter SortedKVIter
	stop []byte
	desc bool
}

func (kv *KV) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	iter, err := kv.Seek(start)
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

最后修改 KV 结构的相关方法，使用 Mem 提供的方法进行实现。