package database

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/mooncell-bb/db_in_45_steps/storage"
)

type DB struct {
	KV     storage.KV
	Tables map[string]Schema
}

func (db *DB) Open() error {
	db.Tables = make(map[string]Schema)
	return db.KV.Open()
}

func (db *DB) Close() error {
	return db.KV.Close()
}

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema, 0)

	val, ok, err := db.KV.Get(key)

	if err != nil || !ok {
		return ok, err
	}

	if err = row.DecodeVal(schema, val); err != nil {
		return false, err
	}

	return true, nil
}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	return db.update(schema, row, storage.ModeInsert)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	return db.update(schema, row, storage.ModeUpsert)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	return db.update(schema, row, storage.ModeUpdate)
}

func (db *DB) update(schema *Schema, row Row, mode storage.UpdateMode) (updated bool, err error) {
	key := row.EncodeKey(schema, 0)
	val := row.EncodeVal(schema)

	oldVal, exist, err := db.KV.Get(key)
	if err != nil {
		return false, err
	}

	switch mode {
	case storage.ModeUpsert:
		updated = !exist || !bytes.Equal(oldVal, val)
	case storage.ModeInsert:
		updated = !exist
	case storage.ModeUpdate:
		updated = exist && !bytes.Equal(oldVal, val)
	default:
		panic("unreachable")
	}
	if !updated {
		return false, nil
	}

	if exist {
		oldRow := row.CopyRow()
		if err = oldRow.DecodeVal(schema, oldVal); err != nil {
			return false, err
		}

		if _, err = db.Delete(schema, oldRow); err != nil {
			return false, err
		}
	}

	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		if i > 0 {
			key, val = row.EncodeKey(schema, i), nil
		}

		updated, err = db.KV.SetEx(key, val, storage.ModeInsert)
		if err == nil && !updated {
			panic("impossible")
		}
	}

	return updated, err
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		key := row.EncodeKey(schema, i)
		deleted, err = db.KV.Del(key)
		if err == nil && !deleted {
			if i != 0 {
				return false, errors.New("inconsistent index")
			}
			break
		}
	}

	return deleted, err
}

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.Tables[table]

	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))

		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}

		if err != nil {
			return Schema{}, err
		}

		if !ok {
			return Schema{}, errors.New("table is not found")
		}

		db.Tables[table] = schema
	}

	return schema, nil
}

type RowIterator struct {
	db      *DB
	schema  *Schema
	indexNo int
	iter    *storage.RangedKVIter
	valid   bool
	row     Row
}

func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {
	start := make([]Cell, len(schema.Indices[0]))
	for i, idx := range schema.Indices[0] {
		if row[idx].Type != schema.Cols[idx].Type {
			panic("cell type mismatch")
		}

		start[i] = row[idx]
	}

	return db.Range(schema, &RangeReq{
		StartCmp: OP_GE,
		StopCmp:  OP_LE,
		Start:    start,
		Stop:     nil,
	})
}

func (db *DB) Range(schema *Schema, req *RangeReq) (out *RowIterator, err error) {
	startDescending, stopDescending := IsDescending(req.StartCmp), IsDescending(req.StopCmp)
	if startDescending == stopDescending {
		panic("operator conflict")
	}

	start := EncodeKeyPrefix(schema, req.IndexNo, req.Start, SuffixPositive(req.StartCmp))
	stop := EncodeKeyPrefix(schema, req.IndexNo, req.Stop, SuffixPositive(req.StopCmp))

	out = &RowIterator{db: db, schema: schema, indexNo: req.IndexNo, row: schema.NewRow()}
	if out.iter, err = db.KV.Range(start, stop, startDescending); err != nil {
		return nil, err
	}

	if out.valid, err = out.decodeKVIter(); err != nil {
		return nil, err
	}
	
	return out, nil
}

func (iter *RowIterator) Next() (err error) {
	if err = iter.iter.Next(); err != nil {
		return err
	}

	iter.valid, err = iter.decodeKVIter()
	return err
}

func (iter *RowIterator) Valid() bool {
	return iter.valid
}

func (iter *RowIterator) Row() Row {
	return iter.row
}

func (iter *RowIterator) decodeKVIter() (bool, error) {
	if !iter.iter.Valid() {
		return false, nil
	}

	if err := iter.row.DecodeKey(iter.schema, iter.indexNo, iter.iter.Key()); err != nil {
		if err == ErrOutOfRange {
			panic("iter out of range err")
		}
		return false, err
	}

	if iter.indexNo > 0 {
		ok, err := iter.db.Select(iter.schema, iter.row)
		if err != nil {
			return false, err
		} else if !ok {
			return false, errors.New("inconsistent index")
		}
	} else {
		if err := iter.row.DecodeVal(iter.schema, iter.iter.Val()); err != nil {
			return false, err
		}
	}

	return true, nil
}
