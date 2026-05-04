package parser

import (
	"errors"
	"slices"

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

func MatchCmp(cond any) (database.ExprOp, []string, []database.Cell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok {
		return 0, nil, nil, false
	}

	switch binop.op {
	case database.OP_LE, database.OP_GE, database.OP_LT, database.OP_GT:
	default:
		return 0, nil, nil, false
	}

	op := binop.op
	left, right := binop.left, binop.right

	names, ok := AsNameList(left)
	if !ok {
		left, right = right, left
		names, ok = AsNameList(left)
		switch op {
		case database.OP_LE:
			op = database.OP_GE
		case database.OP_GE:
			op = database.OP_LE
		case database.OP_LT:
			op = database.OP_GT
		case database.OP_GT:
			op = database.OP_LT
		}
	}

	if !ok {
		return 0, nil, nil, false
	}

	cells, ok := AsCellList(right)
	if !ok {
		return 0, nil, nil, false
	}

	return op, names, cells, true
}

func MatchRange(schema *database.Schema, cond any) (*database.RangeReq, bool) {
	for indexNo := range schema.Indices {
		if req, ok := MatchRangeByIndex(schema, indexNo, cond); ok {
			return req, ok
		}
	}

	return nil, false
}

func MatchRangeByIndex(schema *database.Schema, indexNo int, cond any) (*database.RangeReq, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == database.OP_AND {
		op1, cols1, cells1, ok := MatchCmp(binop.left)
		if !ok || !database.IsPKeyPrefix(schema, indexNo, cols1, cells1) {
			return nil, false
		}

		op2, cols2, cells2, ok := MatchCmp(binop.left)
		if !ok || !database.IsPKeyPrefix(schema, indexNo, cols2, cells2) {
			return nil, false
		}

		if database.IsDescending(op1) != database.IsDescending(op2) {
			return nil, false
		}

		if database.IsDescending(op1) {
			op1, op2, cells1, cells2 = op2, op1, cells2, cells1
		}

		return &database.RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     cells2,
			IndexNo:  indexNo,
		}, true
	} else if ok {
		op1, cols1, cells1, ok := MatchCmp(cond)
		if !ok || !database.IsPKeyPrefix(schema, indexNo, cols1, cells1) {
			return nil, false
		}

		op2 := database.OP_LE
		if database.IsDescending(op1) {
			op2 = database.OP_GE
		}

		return &database.RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     nil,
			IndexNo:  indexNo,
		}, true
	}
	return nil, false
}

func AsNameList(expr any) (out []string, ok bool) {
	switch e := expr.(type) {
	case string:
		return []string{e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(string); ok {
				out = append(out, s)
			} else {
				return nil, false
			}
		}
		return out, true
	}

	return nil, false
}

func AsCellList(expr any) (out []database.Cell, ok bool) {
	switch e := expr.(type) {
	case *database.Cell:
		return []database.Cell{*e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(*database.Cell); ok {
				out = append(out, *s)
			} else {
				return nil, false
			}
		}
		return out, true
	}

	return nil, false
}

func MakeRange(schema *database.Schema, cond any) (*database.RangeReq, error) {
	if keys, ok := MatchAllEq(cond, nil); ok {
		if pkey, ok := database.ExtractPKey(schema, keys); ok {
			return &database.RangeReq{
				StartCmp: database.OP_GE,
				StopCmp:  database.OP_LE,
				Start:    pkey,
				Stop:     pkey,
			}, nil
		}
	}

	if req, ok := MatchRange(schema, cond); ok {
		return req, nil
	}

	return nil, errors.New("unimplemented WHERE")
}

func AddPKeyToIndex(index []int, pkey []int) []int {
	for _, idx := range pkey {
		if !slices.Contains(index, idx) {
			index = append(index, idx)
		}
	}

	return index
}
