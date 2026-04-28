package storage

import (
	"bytes"
	"slices"
)

type SortedArray struct {
	Keys    [][]byte
	Vals    [][]byte
	Deleted []bool
}

func (arr *SortedArray) Size() int {
	return len(arr.Keys)
}

func (arr *SortedArray) EstimatedSize() int {
	return len(arr.Keys)
}

func (arr *SortedArray) Clear() {
	arr.Keys = arr.Keys[:0]
	arr.Vals = arr.Vals[:0]
	arr.Deleted = arr.Deleted[:0]
}

func (arr *SortedArray) Push(key []byte, val []byte, deleted bool) {
	arr.Keys = append(arr.Keys, key)
	arr.Vals = append(arr.Vals, val)
	arr.Deleted = append(arr.Deleted, deleted)
}

func (arr *SortedArray) Pop() {
	n := arr.Size()
	arr.Keys = arr.Keys[:n-1]
	arr.Vals = arr.Vals[:n-1]
	arr.Deleted = arr.Deleted[:n-1]
}

func (arr *SortedArray) Key(i int) []byte {
	return arr.Keys[i]
}

func (arr *SortedArray) Set(key []byte, val []byte) (updated bool, err error) {
	idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	updated = !ok || arr.Deleted[idx] || !bytes.Equal(val, arr.Vals[idx])
	if updated {
		if ok {
			arr.Vals[idx] = val
			arr.Deleted[idx] = false
		} else {
			arr.Keys = slices.Insert(arr.Keys, idx, key)
			arr.Vals = slices.Insert(arr.Vals, idx, val)
			arr.Deleted = slices.Insert(arr.Deleted, idx, false)
		}
	}

	return updated, nil
}

func (arr *SortedArray) Del(key []byte) (deleted bool, err error) {
	idx, ok := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	exist := ok && !arr.Deleted[idx]
	if exist {
		arr.Vals[idx] = nil
		arr.Deleted[idx] = true

		return true, nil
	} else {
		arr.Keys = slices.Insert(arr.Keys, idx, key)
		arr.Vals = slices.Insert(arr.Vals, idx, nil)
		arr.Deleted = slices.Insert(arr.Deleted, idx, true)

		return false, nil
	}
}

type SortedArrayIter struct {
	Keys    [][]byte
	Vals    [][]byte
	deleted []bool
	pos     int
}

func (arr *SortedArray) Seek(key []byte) (SortedKVIter, error) {
	pos, _ := slices.BinarySearchFunc(arr.Keys, key, bytes.Compare)

	return &SortedArrayIter{Keys: arr.Keys, Vals: arr.Vals, deleted: arr.Deleted, pos: pos}, nil
}

func (arr *SortedArray) Iter() (SortedKVIter, error) {
	return &SortedArrayIter{arr.Keys, arr.Vals, arr.Deleted, 0}, nil
}

func (iter *SortedArrayIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.Keys)
}

func (iter *SortedArrayIter) Key() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter Error")
	}

	return iter.Keys[iter.pos]
}

func (iter *SortedArrayIter) Val() []byte {
	if !iter.Valid() {
		panic("SortedArrayIter Error")
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

func (iter *SortedArrayIter) Deleted() bool {
	if !iter.Valid() {
		panic("SortedArrayIter error")
	}

	return iter.deleted[iter.pos]
}
