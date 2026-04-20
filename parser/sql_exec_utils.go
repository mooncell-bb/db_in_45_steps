package parser

import (
	"errors"

	"github.com/mooncell-bb/db_in_45_steps/database"
)

func MatchPKey(schema *database.Schema, cond any) (database.Row, error) {
	if keys, ok := MatchAllEq(cond, nil); ok {
		return database.MakePKey(schema, keys)
	}
	
	return nil, errors.New("unimplemented WHERE")
}

func MatchAllEq(cond any, out []database.NamedCell) ([]database.NamedCell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == database.OP_AND {
		if out, ok = MatchAllEq(binop.left, out); !ok {
			return nil, false
		}

		if out, ok = MatchAllEq(binop.right, out); !ok {
			return nil, false
		}

		return out, true
	} else if ok && binop.op == database.OP_EQ {
		left, right := binop.left, binop.right

		name, ok := left.(string)
		if !ok {
			left, right = right, left
			name, ok = left.(string)
		}

		if !ok {
			return nil, false
		}

		cell, ok := right.(*database.Cell)
		if !ok {
			return nil, false
		}

		return append(out, database.NamedCell{Column: name, Value: *cell}), true
	}
	
	return nil, false
}
