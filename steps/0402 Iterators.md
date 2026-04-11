在 storage/kv 中，为 KV 结构体新增迭代器：

```go
type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int
}
```

同时新增迭代器相关方法：

- func (kv \*KV) Seek(key []byte) (\*KVIterator, error)
- func (iter *KVIterator) Valid() bool
- func (iter *KVIterator) Key() []byte
- func (iter *KVIterator) Val() []byte
- func (iter *KVIterator) Next() error
- func (iter *KVIterator) Prev() error

之后就可以使用迭代器进行迭代遍历：

```go
for iter, err = kv.Seek(start); err == nil && iter.Valid(); err = iter.Next() {
    key, val := iter.Key(), iter.Val()
    
    ...
}
```

