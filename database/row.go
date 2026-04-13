package database

import (
	"errors"
	"slices"
)

type Schema struct {
	Table string
	Cols  []Column
	PKey  []int
}

type Column struct {
	Name string
	Type CellType
}

type Row []Cell

func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

func (row Row) EncodeKey(schema *Schema) (key []byte) {
	if len(row) != len(schema.Cols) {
		panic("mismatch between row data and schema")
	}

	key = append([]byte(schema.Table), 0x00)

	for _, idx := range schema.PKey {
		cell := row[idx]
		if cell.Type != schema.Cols[idx].Type {
			panic("cell type mismatch")
		}

		key = cell.EncodeKey(key)
	}

	return key
}

func (row Row) EncodeVal(schema *Schema) (val []byte) {
	if len(row) != len(schema.Cols) {
		panic("mismatch between row data and schema")
	}

	for idx, cell := range row {
		if cell.Type != schema.Cols[idx].Type {
			panic("cell type mismatch")
		}

		if !slices.Contains(schema.PKey, idx) {
			val = cell.EncodeVal(val)
		}
	}

	return val
}

var ErrOutOfRange = errors.New("out of range")

func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	if len(key) < len(schema.Table)+1 {
		return ErrOutOfRange
	}

	index := slices.Index(key, 0x00)
	if index == -1 {
		return errors.New("cannot find table info")
	}

	table := string(key[:index])
	if table != schema.Table {
		return ErrOutOfRange
	}

	if len(row) != len(schema.Cols) {
		panic("decode key failure")
	}

	key = key[len(schema.Table)+1:]

	for _, idx := range schema.PKey {
		col := schema.Cols[idx]
		row[idx] = Cell{Type: col.Type}

		if key, err = row[idx].DecodeKey(key); err != nil {
			return err
		}
	}

	if len(key) != 0 {
		return errors.New("trailing garbage")
	}

	return nil
}

func (row Row) DecodeVal(schema *Schema, val []byte) (err error) {
	if len(row) != len(schema.Cols) {
		panic("mismatch between row data and schema")
	}

	for idx, col := range schema.Cols {
		if slices.Contains(schema.PKey, idx) {
			continue
		}

		row[idx] = Cell{Type: col.Type}

		if val, err = row[idx].DecodeVal(val); err != nil {
			return err
		}
	}

	if len(val) != 0 {
		return errors.New("trailing garbage")
	}

	return nil
}
