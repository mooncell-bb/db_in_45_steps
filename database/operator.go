package database

type ExprOp uint8

const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_MUL ExprOp = 3  // *
	OP_DIV ExprOp = 4  // /
	OP_EQ  ExprOp = 10 // =
	OP_NE  ExprOp = 11 // !=
	OP_LE  ExprOp = 12 // <=
	OP_GE  ExprOp = 13 // >=
	OP_LT  ExprOp = 14 // <
	OP_GT  ExprOp = 15 // >
	OP_AND ExprOp = 20 // AND
	OP_OR  ExprOp = 21 // OR
	OP_NOT ExprOp = 30 // not
	OP_NEG ExprOp = 31 // -
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

func ExpropToStr(op ExprOp) string {
	switch op {
	case OP_ADD:
		return "+"
	case OP_SUB:
		return "-"
	case OP_MUL:
		return "*"
	case OP_DIV:
		return "/"
	case OP_EQ:
		return "="
	case OP_NE:
		return "!="
	case OP_LE:
		return "<="
	case OP_GE:
		return ">="
	case OP_LT:
		return "<"
	case OP_GT:
		return ">"
	case OP_AND:
		return "AND"
	case OP_OR:
		return "OR"
	case OP_NOT:
		return "NOT"
	case OP_NEG:
		return "-"
	default:
		panic("unreachable")
	}
}
