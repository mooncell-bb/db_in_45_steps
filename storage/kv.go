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
	Log  Log
	Keys [][]byte
	Vals [][]byte
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

	kv.Keys, kv.Vals = kv.Keys[:0], kv.Vals[:0]
	for _, ent := range entries {
		n := len(kv.Keys)
		if n > 0 && bytes.Equal(kv.Keys[n-1], ent.key) {
			kv.Keys, kv.Vals = kv.Keys[:n-1], kv.Vals[:n-1]
		}

		if !ent.deleted {
			kv.Keys = append(kv.Keys, ent.key)
			kv.Vals = append(kv.Vals, ent.val)
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.Log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare); ok {
		return kv.Vals[idx], true, nil
	}

	return nil, false, nil
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	idx, exist := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare)

	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(kv.Vals[idx], val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(kv.Vals[idx], val)
	default:
		panic("update mode mismatch")
	}

	if updated {
		if err = kv.Log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}

		if exist {
			kv.Vals[idx] = val
		} else {
			kv.Keys = slices.Insert(kv.Keys, idx, key)
			kv.Vals = slices.Insert(kv.Vals, idx, val)
		}
	}

	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare); ok {
		if err := kv.Log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err
		}

		kv.Keys = slices.Delete(kv.Keys, idx, idx+1)
		kv.Vals = slices.Delete(kv.Vals, idx, idx+1)
		return true, nil
	}

	return false, nil
}

type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

func (kv *KV) Seek(key []byte) (*KVIterator, error) {
	pos, _ := slices.BinarySearchFunc(kv.Keys, key, bytes.Compare)

	return &KVIterator{keys: kv.Keys, vals: kv.Vals, pos: pos}, nil

}

func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

func (iter *KVIterator) Key() []byte {
	if iter.Valid() {
		return iter.keys[iter.pos]
	}

	return nil
}

func (iter *KVIterator) Val() []byte {
	if iter.Valid() {
		return iter.vals[iter.pos]
	}

	return nil
}

func (iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos++
	}

	return nil
}

func (iter *KVIterator) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}

	return nil
}
