当前己经实现了 SortedArray 和 SortedFile 作为数据库的存储源，且内存数组的数据源比磁盘数组具有更高优先级。由于一个键可能同时存在于内存数组和磁盘数组中，因此不能直接删除内存数组的键。

要实现的 LSM-Tree 中，磁盘数组可以有多层。查询单个键时，需要从上到下逐层搜索。但对于范围查询，必须在所有层级的迭代器中选取最小的键。

创建 merge.go 文件，实现 MergedSortedKV 结构体，用于合并多个迭代器。上层级别比下层级别具有更高的优先级，且 which 属性记录当前具有最小或最大键的迭代器。

```
storage                 
├─ kv.go                
├─ kv_entry.go          
├─ kv_test.go           
├─ log.go               
├─ merge.go             
├─ os_other.go          
├─ os_unix.go           
├─ sorted_array.go      
├─ sorted_file.go       
├─ sorted_file_test.go  
└─ sort_interface.go    
```

```go
type MergedSortedKV []SortedKV

func (m MergedSortedKV) Iter() (iter SortedKVIter, err error) {
    levels := make([]SortedKVIter, len(m))
    for i, sub := range m {
        if levels[i], err = sub.Iter(); err != nil {
            return nil, err
        }
    }
    return &MergedSortedKVIter{levels, levelsLowest(levels)}, nil
}

type MergedSortedKVIter struct {
	levels []SortedKVIter
	which  int
}

func (iter *MergedSortedKVIter) Valid() bool {
    return iter.which >= 0
}

func (iter *MergedSortedKVIter) Key() []byte {
    return iter.levels[iter.which].Key()
}

func (iter *MergedSortedKVIter) Val() []byte {
    return iter.levels[iter.which].Val()
}
```

- func (iter *MergedSortedKVIter) Next() error
- func (iter *MergedSortedKVIter) Prev() error