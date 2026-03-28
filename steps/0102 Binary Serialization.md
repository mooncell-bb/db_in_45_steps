KV 键值对需要序列化为字节序列，以便将其存储到磁盘或通过网络传输。

新增 storage/kv_entry.go 文件，其中包含 Entry 结构体，并以特定方式将其序列化和反序列化。

```
storage         
├─ kv.go        
├─ kv_entry.go  
└─ kv_test.go   
```

```go
type Entry struct {
	key []byte
	val []byte
}
```

```
| key size | val size | key data | val data |
| 4 bytes  | 4 bytes  |   ...    |   ...    |
```

实现 Entry.Encode() 和 Entry.Decode() 方法：

- func (ent *Entry) Encode() []byte
- func (ent *Entry) Decode(r io.Reader) error

使用 binary.LittleEndian.PutUint32() 方法存储 KV 大小。

使用 binary.LittleEndian.Uint32() 方法获取 KV 大小，io.ReadFull() 函数读取指定长度数据。