package database

import (
	"errors"
	"slices"
)

func LookupColumns(cols []Column, names []string) (indices []int, err error) {
	for _, name := range names {
		idx := slices.IndexFunc(cols, func(col Column) bool {
			return col.Name == name
		})

		if idx < 0 {
			return nil, errors.New("column is not found")
		}

		indices = append(indices, idx)
	}

	return
}

func MakePKey(schema *Schema, pkey []NamedCell) (Row, error) {
	if len(schema.PKey) != len(pkey) {
		return nil, errors.New("not primary key")
	}

	row := schema.NewRow()
	for _, idx1 := range schema.PKey {
		col := schema.Cols[idx1]

		idx2 := slices.IndexFunc(pkey, func(expr NamedCell) bool {
			return expr.Column == col.Name && expr.Value.Type == col.Type
		})

		if idx2 < 0 {
			return nil, errors.New("not primary key")
		}

		row[idx1] = pkey[idx2].Value
	}

	return row, nil
}

func FillNonPKey(schema *Schema, updates []NamedCell, out Row) error {
	for _, expr := range updates {
		idx := slices.IndexFunc(schema.Cols, func(col Column) bool {
			return col.Name == expr.Column && col.Type == expr.Value.Type
		})

		if idx < 0 || slices.Contains(schema.PKey, idx) {
			return errors.New("cannot update column")
		}

		out[idx] = expr.Value
	}

	return nil
}

func SubsetRow(row Row, indices []int) (out Row) {
	for _, idx := range indices {
		out = append(out, row[idx])
	}

	return
}
