package database

type ExprOp uint8

const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_LE  ExprOp = 12 // <=
	OP_GE  ExprOp = 13 // >=
	OP_LT  ExprOp = 14 // <
	OP_GT  ExprOp = 15 // >
)

type RangeReq struct {
	StartCmp ExprOp
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
}

func IsDescending(op ExprOp) bool {
	switch op {
	case OP_LE, OP_LT:
		return true
	case OP_GE, OP_GT:
		return false
	default:
		panic("unreachable")
	}
}

func SuffixPositive(op ExprOp) bool {
	switch op {
	case OP_LE, OP_GT:
		return true
	case OP_GE, OP_LT:
		return false
	default:
		panic("unreachable")
	}
}
