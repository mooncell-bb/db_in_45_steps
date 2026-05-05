package database

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/mooncell-bb/db_in_45_steps/storage"
)

type DB struct {
	KV storage.KV
}

func (db *DB) Open() error {
	return db.KV.Open()
}

func (db *DB) Close() error {
	return db.KV.Close()
}

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	tx := db.NewTX()
	defer tx.Abort()

	return tx.Select(schema, row)
}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.Insert(schema, row)

	return storage.AbortOrCommit(tx, updated, err)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.Upsert(schema, row)

	return storage.AbortOrCommit(tx, updated, err)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.Update(schema, row)

	return storage.AbortOrCommit(tx, updated, err)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	tx := db.NewTX()
	deleted, err = tx.Delete(schema, row)

	return storage.AbortOrCommit(tx, deleted, err)
}

func (db *DB) GetSchema(table string) (Schema, error) {
	tx := db.NewTX()
	defer tx.Abort()

	return tx.GetSchema(table)
}

type RowIterator struct {
	tx      *DBTX
	schema  *Schema
	indexNo int
	iter    *storage.RangedKVIter
	valid   bool
	row     Row
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
		ok, err := iter.tx.Select(iter.schema, iter.row)
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

type DBTX struct {
	KV     *storage.KVTX
	Tables map[string]Schema
}

func (db *DB) NewTX() *DBTX {
	return &DBTX{
		KV:     db.KV.NewTX(),
		Tables: map[string]Schema{},
	}
}

func (tx *DBTX) Abort() {
	tx.KV.Abort()
}

func (tx *DBTX) Commit() error {
	return tx.KV.Commit()
}

func (tx *DBTX) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema, 0)
	val, ok, err := tx.KV.Get(key)
	if err != nil || !ok {
		return ok, err
	}

	if err = row.DecodeVal(schema, val); err != nil {
		return false, err
	}

	return true, nil
}

func (tx *DBTX) update(schema *Schema, row Row, mode storage.UpdateMode) (updated bool, err error) {
	key := row.EncodeKey(schema, 0)
	val := row.EncodeVal(schema)
	oldVal, exist, err := tx.KV.Get(key)
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
		if _, err = tx.Delete(schema, oldRow); err != nil {
			return false, err
		}
	}

	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		if i > 0 {
			key, val = row.EncodeKey(schema, i), nil
		}
		updated, err = tx.KV.SetEx(key, val, storage.ModeInsert)
		if err == nil && !updated {
			panic("impossible")
		}
	}

	return updated, err
}

func (tx *DBTX) Insert(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeInsert)
}

func (tx *DBTX) Upsert(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeUpsert)
}

func (tx *DBTX) Update(schema *Schema, row Row) (updated bool, err error) {
	return tx.update(schema, row, storage.ModeUpdate)
}

func (tx *DBTX) Delete(schema *Schema, row Row) (deleted bool, err error) {
	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		key := row.EncodeKey(schema, i)
		deleted, err = tx.KV.Del(key)
		if err == nil && !deleted {
			if i != 0 {
				return false, errors.New("inconsistent index")
			}
			break
		}
	}
	return deleted, err
}

func (tx *DBTX) GetSchema(table string) (Schema, error) {
	schema, ok := tx.Tables[table]
	if !ok {
		val, ok, err := tx.KV.Get([]byte("@schema_" + table))
		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table is not found")
		}
		tx.Tables[table] = schema
	}

	return schema, nil
}

func (tx *DBTX) Seek(schema *Schema, row Row) (*RowIterator, error) {
	start := make([]Cell, len(schema.Indices[0]))
	for i, idx := range schema.Indices[0] {
		if row[idx].Type != schema.Cols[idx].Type {
			panic("cell type mismatch")
		}

		start[i] = row[idx]
	}

	return tx.Range(schema, &RangeReq{
		StartCmp: OP_GE,
		StopCmp:  OP_LE,
		Start:    start,
		Stop:     nil,
	})
}

func (tx *DBTX) Range(schema *Schema, req *RangeReq) (out *RowIterator, err error) {
	startDescending, stopDescending := IsDescending(req.StartCmp), IsDescending(req.StopCmp)
	if startDescending == stopDescending {
		panic("operator conflict")
	}

	start := EncodeKeyPrefix(schema, req.IndexNo, req.Start, SuffixPositive(req.StartCmp))
	stop := EncodeKeyPrefix(schema, req.IndexNo, req.Stop, SuffixPositive(req.StopCmp))

	out = &RowIterator{tx: tx, schema: schema, indexNo: req.IndexNo, row: schema.NewRow()}
	if out.iter, err = tx.KV.Range(start, stop, startDescending); err != nil {
		return nil, err
	}

	if out.valid, err = out.decodeKVIter(); err != nil {
		return nil, err
	}

	return out, nil
}
