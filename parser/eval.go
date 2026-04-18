package parser

import (
	"bytes"
	"cmp"
	"errors"
	"slices"

	"github.com/mooncell-bb/db_in_45_steps/database"
)

func evalExpr(schema *database.Schema, row database.Row, expr any) (*database.Cell, error) {
	switch e := expr.(type) {
	case string:
		idx := slices.IndexFunc(schema.Cols, func(col database.Column) bool {
			return col.Name == e
		})

		if idx < 0 {
			return nil, errors.New("unknown colnum")
		}
		return &row[idx], nil
	case *database.Cell:
		return e, nil
	case *ExprUnOp:
		kid, err := evalExpr(schema, row, e.kid)
		if err != nil {
			return nil, err
		}

		if e.op == database.OP_NEG && kid.Type == database.TypeI64 {
			return &database.Cell{Type: database.TypeI64, I64: -kid.I64}, nil
		} else if e.op == database.OP_NOT && kid.Type == database.TypeI64 {
			res := int64(0)
			if kid.I64 == 0 {
				res = 1
			}

			return &database.Cell{Type: database.TypeI64, I64: res}, nil
		}

		return nil, errors.New("bad unary op")
	case *ExprBinOp:
		left, err := evalExpr(schema, row, e.left)
		if err != nil {
			return nil, err
		}

		right, err := evalExpr(schema, row, e.right)
		if err != nil {
			return nil, err
		}

		if left.Type != right.Type {
			return nil, errors.New("binary op type mismatch")
		}

		out := &database.Cell{Type: left.Type}

		switch e.op {
		case database.OP_EQ, database.OP_NE, database.OP_LE, database.OP_GE, database.OP_LT, database.OP_GT:
			res := 0
			switch out.Type {
			case database.TypeI64:
				res = cmp.Compare(left.I64, right.I64)
			case database.TypeStr:
				res = bytes.Compare(left.Str, right.Str)
			default:
				panic("unknown type")
			}

			b := false
			switch e.op {
			case database.OP_EQ:
				b = (res == 0)
			case database.OP_NE:
				b = (res != 0)
			case database.OP_LE:
				b = (res <= 0)
			case database.OP_GE:
				b = (res >= 0)
			case database.OP_LT:
				b = (res < 0)
			case database.OP_GT:
				b = (res > 0)
			}

			if b {
				out.I64 = 1
			}

			return out, nil
		}

		switch {
		case e.op == database.OP_ADD && out.Type == database.TypeStr:
			out.Str = slices.Concat(left.Str, right.Str)

		case e.op == database.OP_ADD && out.Type == database.TypeI64:
			out.I64 = left.I64 + right.I64
		case e.op == database.OP_SUB && out.Type == database.TypeI64:
			out.I64 = left.I64 - right.I64
		case e.op == database.OP_MUL && out.Type == database.TypeI64:
			out.I64 = left.I64 * right.I64
		case e.op == database.OP_DIV && out.Type == database.TypeI64:
			if right.I64 == 0 {
				return nil, errors.New("division by 0")
			}
			out.I64 = left.I64 / right.I64

		case e.op == database.OP_AND && out.Type == database.TypeI64:
			if left.I64 != 0 && right.I64 != 0 {
				out.I64 = 1
			}
		case e.op == database.OP_OR && out.Type == database.TypeI64:
			if left.I64 != 0 || right.I64 != 0 {
				out.I64 = 1
			}
		default:
			return nil, errors.New("bad binary op")
		}

		return out, nil
	default:
		panic("unknown expr type")
	}
}
