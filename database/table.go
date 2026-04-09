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
