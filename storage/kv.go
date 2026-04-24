package storage

import (
	"bytes"
	"slices"
)

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

type KV struct {
	Log Log
	Mem SortedArray
}

func (kv *KV) Open() error {
	if err := kv.Log.Open(); err != nil {
		return err
	}

	entries := []Entry{}
	for {
		ent := Entry{}
		if eof, err := kv.Log.Read(&ent); err != nil {
			return err
		} else if eof {
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

		if !ent.deleted {
			kv.Mem.Push(ent.key, ent.val)
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.Log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	return kv.Mem.Get(key)
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

	return kv.Mem.Del(key)
}

func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	return kv.Mem.Seek(key)
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
