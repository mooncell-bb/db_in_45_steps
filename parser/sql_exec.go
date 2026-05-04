package parser

import (
	"encoding/json"
	"errors"

	"github.com/mooncell-bb/db_in_45_steps/database"
)

type Exec struct {
	DB *database.DB
}

type SQLResult struct {
	Updated int
	Header  []string
	Values  []database.Row
}

func (exec *Exec) Open() error {
	return exec.DB.Open()
}

func (exec *Exec) Close() error {
	return exec.DB.KV.Close()
}

func (exec *Exec) ExecStmt(stmt any) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreatTable:
		err = exec.execCreateTable(ptr)
	case *StmtSelect:
		r.Header = ExprsToHeader(ptr.cols)
		r.Values, err = exec.execSelect(ptr)
	case *StmtInsert:
		r.Updated, err = exec.execInsert(ptr)
	case *StmtUpdate:
		r.Updated, err = exec.execUpdate(ptr)
	case *StmtDelete:
		r.Updated, err = exec.execDelete(ptr)
	default:
		panic("unreachable")
	}
	return
}

func (exec *Exec) execCreateTable(stmt *StmtCreatTable) (err error) {
	if _, err := exec.DB.GetSchema(stmt.table); err == nil {
		return errors.New("duplicate table name")
	}

	schema := database.Schema{
		Table: stmt.table,
		Cols:  stmt.cols,
	}
	for i, names := range append([][]string{stmt.pkey}, stmt.indices...) {
		index, err := database.LookupColumns(stmt.cols, names)
		if err != nil {
			return err
		}
		if i > 0 {
			index = AddPKeyToIndex(index, schema.Indices[0])
		}

		schema.Indices = append(schema.Indices, index)
	}

	val, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	if _, err = exec.DB.KV.Set([]byte("@schema_"+stmt.table), val); err != nil {
		return err
	}

	exec.DB.Tables[schema.Table] = schema
	return nil
}

func (exec *Exec) execInsert(stmt *StmtInsert) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	if len(schema.Cols) != len(stmt.value) {
		return 0, errors.New("schema mismatch")
	}

	for i := range schema.Cols {
		if schema.Cols[i].Type != stmt.value[i].Type {
			return 0, errors.New("schema mismatch")
		}
	}

	updated, err := exec.DB.Insert(&schema, stmt.value)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}

	return count, nil
}

func (exec *Exec) execDelete(stmt *StmtDelete) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	deletes := make([]database.Row, 0)
	for ; err == nil && iter.Valid(); err = iter.Next() {
		deletes = append(deletes, iter.Row().CopyRow())
	}

	for _, row := range deletes {
		updated, err := exec.DB.Delete(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}

	if err != nil {
		return 0, err
	}

	return count, nil
}

func (exec *Exec) execUpdate(stmt *StmtUpdate) (count int, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	oldRows := []database.Row{}
	for ; err == nil && iter.Valid(); err = iter.Next() {
		oldRows = append(oldRows, iter.Row().CopyRow())
	}

	if err != nil {
		return 0, nil
	}

	for _, row := range oldRows {
		updates := make([]database.NamedCell, len(stmt.value))
		for i, assign := range stmt.value {
			cell, err := evalExpr(&schema, row, assign.expr)
			if err != nil {
				return 0, nil
			}

			updates[i] = database.NamedCell{Column: assign.column, Value: *cell}
		}

		if err = database.FillNonPKey(&schema, updates, row); err != nil {
			return 0, err
		}

		updated, err := exec.DB.Update(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}

	return count, nil
}

func (exec *Exec) execSelect(stmt *StmtSelect) (output []database.Row, err error) {
	schema, err := exec.DB.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	iter, err := exec.execCond(&schema, stmt.cond)
	if err != nil {
		return nil, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()

		computed := make(database.Row, len(stmt.cols))
		for i, expr := range stmt.cols {
			cell, err := evalExpr(&schema, row, expr)
			if err != nil {
				return nil, err
			}

			computed[i] = *cell
		}

		output = append(output, computed)
	}

	if err != nil {
		return nil, err
	}

	return output, nil
}

func (exec *Exec) execCond(schema *database.Schema, cond any) (*database.RowIterator, error) {
	req, err := MakeRange(schema, cond)
	if err != nil {
		return nil, err
	}

	return exec.DB.Range(schema, req)
}
