package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
)

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

type KVOptions struct {
	Dirpath      string
	LogShreshold int
	GrowthFactor float32
}

type KV struct {
	Options KVOptions
	Meta    KVMetaStore
	Version uint64
	Log     Log
	Mem     SortedArray
	Main    []SortedFile
	MultiClosers
}

func (kv *KV) Open() (err error) {
	if err = kv.openAll(); err != nil {
		_ = kv.Close()
	}

	return err
}

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

func (kv *KV) openMeta() error {
	kv.Meta.slots[0].FileName = path.Join(kv.Options.Dirpath, "meta0")
	kv.Meta.slots[1].FileName = path.Join(kv.Options.Dirpath, "meta1")

	if err := kv.Meta.Open(); err != nil {
		return err
	}

	kv.MultiClosers = append(kv.MultiClosers, &kv.Meta)
	return nil
}

func (kv *KV) openLog() error {
	kv.Log.FileName = path.Join(kv.Options.Dirpath, "log")
	if err := kv.Log.Open(); err != nil {
		return err
	}

	kv.MultiClosers = append(kv.MultiClosers, &kv.Log)

	entries := []Entry{}
	for {
		ent := Entry{}
		eof, err := kv.Log.Read(&ent)
		if err != nil {
			if err == ErrBadSum || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		if eof {
			break
		}

		entries = append(entries, ent)
	}

	slices.SortStableFunc(entries, func(a, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})

	kv.Mem.Clear()
	for _, ent := range entries {
		n := kv.Mem.Size()
		if n > 0 && bytes.Equal(kv.Mem.Key(n-1), ent.key) {
			kv.Mem.Pop()
		}

		kv.Mem.Push(ent.key, ent.val, ent.deleted)
	}

	return nil
}

func (kv *KV) openSSTable() error {
	meta := kv.Meta.Get()
	kv.Version = meta.Version
	kv.Main = kv.Main[:0]

	for _, sstable := range meta.SSTables {
		sstable = path.Join(kv.Options.Dirpath, sstable)
		file := &SortedFile{FileName: sstable}
		if err := file.Open(); err != nil {
			return err
		}
		kv.MultiClosers = append(kv.MultiClosers, file)
		kv.Main = append(kv.Main, *file)
	}

	return nil
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	tx := kv.NewTX()
	defer tx.Abort()

	return tx.Get(key)
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	tx := kv.NewTX()
	updated, err = tx.SetEx(key, val, mode)

	return AbortOrCommit(tx, updated, err)
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	tx := kv.NewTX()
	deleted, err = tx.Del(key)

	return AbortOrCommit(tx, deleted, err)
}

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
			i--
			continue
		}
	}

	return nil
}

func (kv *KV) compactLog() error {
	kv.Version++
	sstable := fmt.Sprintf("sstable_%d", kv.Version)
	filename := path.Join(kv.Options.Dirpath, sstable)

	file := &SortedFile{FileName: filename}
	m := SortedKV(&kv.Mem)
	if len(kv.Main) == 0 {
		m = NoDeletedSortedKV{m}
	}

	if err := file.CreateFromSorted(m); err != nil {
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

	kv.Main = slices.Insert(kv.Main, 0, *file)
	kv.MultiClosers = append(kv.MultiClosers, file)
	kv.Mem.Clear()
	return kv.Log.Truncate()
}

func (kv *KV) shouldMerge(idx int) bool {
	if !(idx >= 0 && idx <= len(kv.Main)-2) {
		return false
	}

	cur, next := kv.Main[idx].EstimatedSize(), kv.Main[idx+1].EstimatedSize()
	return float32(cur)*kv.Options.GrowthFactor >= float32(cur+next)
}

func (kv *KV) compactSSTable(level int) error {
	if !(level >= 0 && level <= len(kv.Main)-2) {
		return errors.New("Level Error")
	}

	kv.Version++
	sstable := fmt.Sprintf("sstable_%d", kv.Version)
	filename := path.Join(kv.Options.Dirpath, sstable)

	file := &SortedFile{FileName: filename}
	m := SortedKV(MergedSortedKV{&kv.Main[level], &kv.Main[level+1]})
	if len(kv.Main) == level+2 {
		m = NoDeletedSortedKV{m}
	}

	if err := file.CreateFromSorted(m); err != nil {
		_ = os.Remove(filename)
		return err
	}

	meta := kv.Meta.Get()
	meta.Version = kv.Version
	meta.SSTables = slices.Replace(meta.SSTables, level, level+2, sstable)
	if err := kv.Meta.Set(meta); err != nil {
		_ = file.Close()
		return err
	}

	old1, old2 := kv.Main[level].FileName, kv.Main[level+1].FileName
	kv.Main = slices.Replace(kv.Main, level, level+2, *file)
	kv.MultiClosers = append(kv.MultiClosers, file)
	_ = os.Remove(old1)
	_ = os.Remove(old2)

	return nil
}

func (kv *KV) applyTX(tx *KVTX) error {
	if err := kv.updateLog(tx); err != nil {
		return err
	}

	kv.updateMem(tx)
	return nil
}

func (kv *KV) updateLog(tx *KVTX) error {
	iter, err := tx.updates.Iter()
	for ; err == nil && iter.Valid(); err = iter.Next() {
		err = kv.Log.Write(
			&Entry{
				key:     iter.Key(),
				val:     iter.Val(),
				deleted: iter.Deleted(),
			},
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (kv *KV) updateMem(tx *KVTX) {
	iter, err := tx.updates.Iter()
	for ; err == nil && iter.Valid(); err = iter.Next() {
		if iter.Deleted() {
			_, err = kv.Mem.Del(iter.Key())
		} else {
			_, err = kv.Mem.Set(iter.Key(), iter.Val())
		}
	}

	if err != nil {
		panic("TX iter error")
	}
}

type RangedKVIter struct {
	iter SortedKVIter
	stop []byte
	desc bool
}

func (iter *RangedKVIter) Valid() bool {
	if !iter.iter.Valid() {
		return false
	}

	r := bytes.Compare(iter.iter.Key(), iter.stop)
	if iter.desc && r < 0 {
		return false
	} else if !iter.desc && r > 0 {
		return false
	}

	return true
}

func (iter *RangedKVIter) Key() []byte {
	if iter.Valid() {
		return iter.iter.Key()
	}

	return nil
}

func (iter *RangedKVIter) Val() []byte {
	if iter.Valid() {
		return iter.iter.Val()
	}

	return nil
}

func (iter *RangedKVIter) Next() error {
	if !iter.Valid() {
		return nil
	}

	if iter.desc {
		return iter.iter.Prev()
	} else {
		return iter.iter.Next()
	}
}

type KVTX struct {
	target  *KV
	updates SortedArray
	levels  MergedSortedKV
}

func (kv *KV) NewTX() *KVTX {
	tx := &KVTX{target: kv}
	tx.levels = MergedSortedKV{&tx.updates, &kv.Mem}

	for i := range kv.Main {
		tx.levels = append(tx.levels, &kv.Main[i])
	}

	return tx
}

func (tx *KVTX) Seek(key []byte) (SortedKVIter, error) {
	iter, err := tx.levels.Seek(key)
	if err != nil {
		return nil, err
	}

	return FilterDeleted(iter)
}

func (tx *KVTX) Get(key []byte) (val []byte, ok bool, err error) {
	iter, err := tx.Seek(key)
	ok = err == nil && iter.Valid() && bytes.Equal(iter.Key(), key)

	if ok {
		val = iter.Val()
	}

	return val, ok, err
}

func (tx *KVTX) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	iter, err := tx.Seek(start)
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

func (tx *KVTX) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	oldVal, exist, err := tx.Get(key)
	if err != nil {
		return false, err
	}

	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(oldVal, val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(oldVal, val)
	default:
		panic("unreachable")
	}

	if updated {
		_, err = tx.updates.Set(key, val)
	}

	return
}

func (tx *KVTX) Set(key []byte, val []byte) (updated bool, err error) {
	return tx.SetEx(key, val, ModeUpsert)
}

func (tx *KVTX) Del(key []byte) (deleted bool, err error) {
	if _, exist, err := tx.Get(key); err != nil || !exist {
		return false, err
	}

	_, err = tx.updates.Del(key)
	return true, err
}

func (tx *KVTX) Commit() error {
	return tx.target.applyTX(tx)
}

func (tx *KVTX) Abort() {}
