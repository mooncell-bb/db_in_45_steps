package storage

import (
	"bytes"
)

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

type KV struct {
	Log Log
	Mem map[string][]byte
}

func (kv *KV) Open() error {
	if err := kv.Log.Open(); err != nil {
		return err
	}

	kv.Mem = make(map[string][]byte)
	for {
		ent := Entry{}
		if eof, err := kv.Log.Read(&ent); err != nil {
			return err
		} else if eof {
			break
		}

		if ent.deleted {
			delete(kv.Mem, string(ent.key))
		} else {
			kv.Mem[string(ent.key)] = ent.val
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.Log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	val, ok = kv.Mem[string(key)]
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	prev, exist := kv.Mem[string(key)]
	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(prev, val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(prev, val)
	default:
		panic("update mode mismatch")
	}

	if updated {
		if err = kv.Log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}
		kv.Mem[string(key)] = val
	}

	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, deleted = kv.Mem[string(key)]

	if deleted {
		ent := &Entry{key: key, deleted: true}
		if err = kv.Log.Write(ent); err != nil {
			return false, err
		}

		delete(kv.Mem, string(key))
	}

	return
}
