kv.go 的 KVOptions 结构体中，添加两个参数：

```go
type KVOptions struct {
	Dirpath      string
	LogShreshold int
	GrowthFactor float32
}
```

- LogShreshold：日志中的最大键值数量，超过此限制时会转换为 SSTable。
- GrowthFactor：下一层与当前层之间的尺寸比例。

KV.Compact() 函数利用这两个参数来决定何时合并 SSTables：

```go
func (kv *KV) Compact() error {
	if kv.Mem.Size() >= kv.Options.LogShreshold {
		if err := kv.compactLog(); err != nil {
			return err
		}
	}

	for i := 0; i < len(kv.Main)-1; i++ {
		if kv.shouldMerge(i) {
			if err := kv.compactSSTable(i); err != nil {
				return err
			}
		}
		i--
		continue
	}

	return nil
}
```

- KV.compactLog()：合并内存数组。
- KV.compactSSTable()：合并磁盘数组。
- kv.shouldMerge()：判断是否合并磁盘数组。