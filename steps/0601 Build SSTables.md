将内存中排序好的数组保存到磁盘上，使用格式为：

```
[ n keys | offset1 | offset2 | ... | offsetn | KV1 | KV2 | ... | KVn ]
  8bytes   8bytes
```

在开头存储数组大小，然后记录每个键在文件中偏移量，这个偏移量用于二分查找和迭代。

每个键值对按以下格式存储：

```
[ key length | val length | key data | val data ]
    4bytes       4bytes
```

storage 目录下新建 sorted_file.go 文件，创建 SortedFile 结构体表示一个存储的内存数据结构。

```
storage            
├─ kv.go           
├─ kv_entry.go     
├─ kv_test.go      
├─ log.go          
├─ os_other.go     
├─ os_unix.go      
└─ sorted_file.go  
```

```go
type SortedFile struct {
	FileName string
	fp       *os.File
}
```

SSTable 通过将内存中的数据结构刷新到磁盘，因此调用此结构创建文件时，可以接收一个迭代器并循环来构建文件内容。

storage 目录下新建 sort_interface.go 文件，其中定义迭代器接口：

```
storage               
├─ kv.go              
├─ kv_entry.go        
├─ kv_test.go         
├─ log.go             
├─ os_other.go        
├─ os_unix.go         
├─ sorted_file.go     
└─ sort_interface.go  
```

```go
type SortedKV interface {
	Size() int
	Iter() (SortedKVIter, error)
}

type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Next() error
	Prev() error
}
```

然后创建 SortedFile.CreateFromSorted() 方法，其接收一个 SortedKV 接口：

- func (file *SortedFile) CreateFromSorted(kv SortedKV) (err error)

```go
func (file *SortedFile) CreateFromSorted(kv SortedKV) (err error) {
	if file.fp, err = createFileSync(file.FileName); err != nil {
		return err
	}

	if err = file.writeSortedFile(kv); err != nil {
		_ = file.Close()
	}
	
	return err
}

func (file *SortedFile) Close() error {
	return file.fp.Close()
}
```

实现核心 SortedFile.writeSortedFile() 方法，迭代并写入文件内容，可使用 Go 提供的 WriteAt() 方法指定写入文件的偏移量。

- func (file *SortedFile) writeSortedFile(kv SortedKV) (err error)



