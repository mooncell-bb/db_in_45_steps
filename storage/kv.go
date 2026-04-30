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

	if meta.SSTable != "" {
		kv.Main.FileName = path.Join(kv.Options.Dirpath, meta.SSTable)
		if err := kv.Main.Open(); err != nil {
			return err
		}

		kv.MultiClosers = append(kv.MultiClosers, &kv.Main)
	}

	return nil
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	iter, err := kv.Seek(key)
	if err != nil {
		return nil, false, err
	}

	if iter.Valid() && bytes.Equal(iter.Key(), key) {
		return iter.Val(), true, nil
	}

	return nil, false, nil
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	oldVal, exist, err := kv.Get(key)
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
		if err = kv.Log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}

		_, err = kv.Mem.Set(key, val)
		if err != nil {
			panic("mem set error")
		}
	}

	return updated, nil
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if _, exist, err := kv.Get(key); err != nil || !exist {
		return false, err
	}

	if err = kv.Log.Write(&Entry{key: key, deleted: true}); err != nil {
		return false, err
	}

	_, err = kv.Mem.Del(key)

	return true, nil
}

func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	m := MergedSortedKV{&kv.Mem, &kv.Main}

	iter, err := m.Seek(key)
	if err != nil {
		return nil, err
	}

	return FilterDeleted(iter)
}

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
