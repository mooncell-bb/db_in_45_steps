当前仅包含一个 SSTable 层级，其进行替换的步骤为：

1. 创建临时文件，写入所有数据。
2. 原子替换当前临时文件名称与 SortedFile.FileName。
3. 清除临时文件名称文件，即原有的 SortedFile.FileName 文件。

而 LSM 树包含多个 SSTable 层级和多个文件，因此固定命名方式不再适用。数据库必须记录各层级的文件名列表。

storage 创建 metadata.go 文件，可以采用双缓冲技术实现原子化更新。即保留两个附带版本号和校验和的副本，并轮流更新它们。发生断电后，可以恢复到完好的版本。

```
slot 0 -> [ version 124 | data yyy | crc32 ]
slot 1 -> [ version 123 | data zzz | crc32 ]
```

```go
type KVMetaStore struct {
	slots [2]KVMetaItem
	MultiClosers
}

type KVMetaItem struct {
	FileName string
	fp       *os.File
	data     KVMetaData
}

type KVMetaData struct {
	Version uint64
	SSTable string
}
```

```go
func readMetaFile(fp *os.File) (data KVMetaData, err error) {
	b, err := io.ReadAll(fp)
	if err != nil {
		return KVMetaData{}, err
	}

	if len(b) <= 8 {
		return KVMetaData{}, nil
	}

	sum := binary.LittleEndian.Uint32(b[0:4])
	size := binary.LittleEndian.Uint32(b[4:8])
	if len(b) < 8+int(size) {
		return KVMetaData{}, nil
	}

	if sum != crc32.ChecksumIEEE(b[4:8+size]) {
		return KVMetaData{}, nil
	}

	if err = json.Unmarshal(b[8:8+size], &data); err != nil {
		return KVMetaData{}, nil
	}

	return data, nil
}

func openMetafile(filename string) (fp *os.File, data KVMetaData, err error) {
	if fp, err = createFileSync(filename); err != nil {
		return nil, KVMetaData{}, err
	}

	if data, err = readMetaFile(fp); err != nil {
		_ = fp.Close()
		return nil, KVMetaData{}, err
	}

	return fp, data, nil
}

func (meta *KVMetaStore) Open() error {
	for i := range meta.slots {
		fp, data, err := openMetafile(meta.slots[i].FileName)
		if err != nil {
			_ = meta.Close()
			return err
		}

		meta.slots[i].fp, meta.slots[i].data = fp, data
		meta.MultiClosers = append(meta.MultiClosers, fp)
	}

	return nil
}
```

```go
func writeMetaFile(fp *os.File, data KVMetaData) error {
	b, err := json.Marshal(data)
	if err != nil {
		panic("MetaData JSON Error")
	}

	b = slices.Concat(make([]byte, 8), b)
	binary.LittleEndian.PutUint32(b[4:8], uint32(len(b)-8))
	binary.LittleEndian.PutUint32(b[0:4], crc32.ChecksumIEEE(b[4:]))
	if _, err = fp.WriteAt(b, 0); err != nil {
		return err
	}

	return fp.Sync()
}

func (meta *KVMetaStore) current() int {
	if meta.slots[0].data.Version > meta.slots[1].data.Version {
		return 0
	} else {
		return 1
	}
}

func (meta *KVMetaStore) Set(data KVMetaData) error {
	cur := meta.current()
	if err := writeMetaFile(meta.slots[1-cur].fp, data); err != nil {
		return err
	}

	meta.slots[1-cur].data = data
	return nil
}

func (meta *KVMetaStore) Get() KVMetaData {
	return meta.slots[meta.current()].data
}
```

