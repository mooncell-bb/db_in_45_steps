现在已经可以将一个 SortedKV 序列化到磁盘，这种格式可以直接在磁盘中读取数据而无需反序列化，也不需要完全加载到内存中，现在实现从磁盘数组中读取第 n 个键值对。

首先在 SortedFile 结构体中添加 nkeys 字段，其保存该磁盘数组中存储的键值对，然后在保存时进行赋值：

```go
type SortedFile struct {
	FileName string
	fp       *os.File
	nkeys    int
}
```

实现 SortedFile.index() 方法，给定数组的索引，返回对应的键值对，可以使用 Go 提供的 ReadAt() 方法。

- func (file *SortedFile) index(pos int) (key []byte, val []byte, err error)

然后实现 SortedFile.findPos() 方法，由于磁盘数组是顺序的，因此可以使用二分搜索。

- func (file *SortedFile) findPos(target []byte) (int, error)

最后定义一个 SortedFileIter 用于迭代磁盘数组，Seek() 方法返回的迭代器可以使用接口而非具体类型。

```go
type SortedFileIter struct {
	file *SortedFile
	pos  int
	key  []byte
	val  []byte
}

func (iter *SortedFileIter) loadCurrent() (err error) {
	if iter.Valid() {
		iter.key, iter.val, err = iter.file.index(iter.pos)
	}
    
	return err
}
```

- func (file *SortedFile) Seek(key []byte) (SortedKVIter, error)
- func (file *SortedFile) Iter() (SortedKVIter, error)
- func (iter *SortedFileIter) Valid() bool
- func (iter *SortedFileIter) Key() []byte
- func (iter *SortedFileIter) Val() []byte
- func (iter *SortedFileIter) Next() error
- func (iter *SortedFileIter) Prev() error

