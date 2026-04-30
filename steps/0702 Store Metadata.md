程序应该自行管理数据文件，而用户只需指定一个目录：

```go
type KVOptions struct {
    Dirpath string
}

type KV struct {
    Options KVOptions
    Meta    KVMetaStore
    Log     Log
    Main    SortedFile
    // ...
}
```

SSTable 文件名是动态生成的，并存储在 KVMetaData 中。而 Log 和 KVMetaData 使用固定的文件名。

之前 KV.Compact() 通过 rename() 替换文件，现在使用 KVMetaStore 来记录新文件名，并在成功后删除旧文件。

KV 结构体中新增 Version 全局变量来记录当前版本号，同时修改 KV.Compact() 方法：

```go
type KVOptions struct {
	Dirpath string
}

type KV struct {
	Options KVOptions
	Meta    KVMetaStore
	Version uint64
	Log     Log
	Mem     SortedArray
	Main    SortedFile
	MultiClosers
}
```

```go
func (kv *KV) Compact() error {
	kv.Version++
	sstable := fmt.Sprintf("sstable_%d", kv.Version)
	filename := path.Join(kv.Options.Dirpath, sstable)

	file := SortedFile{FileName: filename}
	m := MergedSortedKV{&kv.Mem, &kv.Main}
	if err := file.CreateFromSorted(m); err != nil {
		_ = os.Remove(filename)
		return err
	}

	meta := kv.Meta.Get()
	meta.Version = kv.Version
	meta.SSTable = sstable
	if err := kv.Meta.Set(meta); err != nil {
		_ = file.Close()
		return err
	}

	_ = kv.Main.Close()
	_ = os.Remove(kv.Main.FileName)

	kv.Main = file
	kv.Mem.Clear()
	return kv.Log.Truncate()
}
```

之前 KV.Open() 方法首先打开 Log，然后打开 SSTable；现在需打开 Meta 信息，然后读取文件信息并打开文件：

- func (kv *KV) openAll() error

```go
func (kv *KV) openAll() error {
	err := os.Mkdir(kv.Options.Dirpath, 0o755)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}

	if err := kv.openMeta(); err != nil {
		return err
	}

	if err := kv.openLog(); err != nil {
		return err
	}

	return kv.openSSTable()
}
```

- func (kv *KV) openMeta() error

```go
func (kv *KV) openMeta() error {
	kv.Meta.slots[0].FileName = path.Join(kv.Options.Dirpath, "meta0")
	kv.Meta.slots[1].FileName = path.Join(kv.Options.Dirpath, "meta1")

	if err := kv.Meta.Open(); err != nil {
		return err
	}

	kv.MultiClosers = append(kv.MultiClosers, &kv.Meta)
	return nil
}
```

- func (kv *KV) openSSTable() error

```go
func (kv *KV) openSSTable() error {
	meta := kv.Meta.Get()
	kv.Version = meta.Version

	if meta.SSTable != "" {
		kv.Main.FileName = path.Join(kv.Options.Dirpath, meta.SSTable)
		if err := kv.Main.Open(); err != nil {
			return err
		}

		kv.MultiClosers = append(kv.MultiClosers, &kv.Main)
	}

	return nil
}
```

KV.openLog() 方法也需要进行修改：

```go
func (kv *KV) openLog() error {
	kv.Log.FileName = path.Join(kv.Options.Dirpath, "log")
	...
}
```

