package storage

import (
	"bytes"
	"slices"
)

type SortedArray struct {
	Keys [][]byte
	Vals [][]byte
}

func (arr *SortedArray) Size() int {
	return len(arr.Keys)
}

func (arr *SortedArray) Iter() (SortedKVIter, error) {
	return &SortedArrayIter{arr.Keys, arr.Vals, 0}, nil
}

func (arr *SortedArray) Clear() {
	arr.Keys, arr.Vals = arr.Keys[:0], arr.Vals[:0]
}

func (arr *SortedArray) Push(key []byte, val []byte) {
	arr.Keys = append(arr.Keys, key)
	arr.Vals = append(arr.Vals, val)
}

func (arr *SortedArray) Pop() {
	n := arr.Size()
	arr.Keys, arr.Vals = arr.Keys[:n-1], arr.Vals[:n-1]
}

func (arr *SortedArray) Key(i int) []byte {
	return arr.Keys[i]
}

func (arr *SortedArray) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare); ok {
		return arr.Vals[idx], true, nil
	}

	return nil, false, nil
}

func (arr *SortedArray) Set(key []byte, val []byte) (updated bool, err error) {
	idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)
	
	updated = !ok || !bytes.Equal(val, arr.Vals[idx])
	if updated {
		if ok {
			arr.Vals[idx] = val
		} else {
			arr.Keys = slices.Insert(arr.Keys, idx, key)
			arr.Vals = slices.Insert(arr.Vals, idx, val)
		}
	}

	return updated, nil
}

func (arr *SortedArray) Del(key []byte) (deleted bool, err error) {
	if idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare); ok {
		arr.Keys = slices.Delete(arr.Keys, idx, idx+1)
		arr.Vals = slices.Delete(arr.Vals, idx, idx+1)

		return true, nil
	}

	return false, nil
}

type SortedArrayIter struct {
	Keys [][]byte
	Vals [][]byte
	pos  int
}

func (arr *SortedArray) Seek(key []byte) (SortedKVIter, error) {
	pos, _ := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	return &SortedArrayIter{Keys: arr.Keys, Vals: arr.Vals, pos: pos}, nil
}

func (iter *SortedArrayIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.Keys)
}

func (iter *SortedArrayIter) Key() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.Keys[iter.pos]
}

func (iter *SortedArrayIter) Val() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.Vals[iter.pos]
}

func (iter *SortedArrayIter) Next() error {
	if iter.pos < len(iter.Keys) {
		iter.pos++
	}

	return nil
}

func (iter *SortedArrayIter) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}

	return nil
}
