package database

import (
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
	key := row.EncodeKey(schema)

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
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, storage.ModeInsert)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, storage.ModeUpsert)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, storage.ModeUpdate)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key := row.EncodeKey(schema)

	return db.KV.Del(key)
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
	schema *Schema
	iter   *storage.KVIterator
	valid  bool
	row    Row
}

func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {
	iter, err := db.KV.Seek(row.EncodeKey(schema))
	if err != nil {
		return nil, err
	}

	valid, err := decodeKVIter(schema, iter, row)
	if err != nil {
		return nil, err
	}

	return &RowIterator{schema, iter, valid, row}, nil
}

func (iter *RowIterator) Next() (err error) {
	if err = iter.iter.Next(); err != nil {
		return err
	}

	iter.valid, err = decodeKVIter(iter.schema, iter.iter, iter.row)
	return err
}

func (iter *RowIterator) Valid() bool {
	return iter.valid
}

func (iter *RowIterator) Row() Row {
	return iter.row
}

func decodeKVIter(schema *Schema, iter *storage.KVIterator, row Row) (bool, error) {
	if !iter.Valid() {
		return false, nil
	}

	if err := row.DecodeKey(schema, iter.Key()); err == ErrOutOfRange {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if err := row.DecodeVal(schema, iter.Val()); err != nil {
		return false, err
	}

	return true, nil
}
