package database

import (
	"encoding/binary"
	"errors"
	"slices"
	"strconv"
)

type CellType uint8

const (
	TypeI64 CellType = 1
	TypeStr CellType = 2
)

type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

type NamedCell struct {
	Column string
	Value  Cell
}

func (cell *Cell) EncodeVal(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.LittleEndian.AppendUint64(toAppend, uint64(cell.I64))
	case TypeStr:
		toAppend = binary.LittleEndian.AppendUint32(toAppend, uint32(len(cell.Str)))
		return append(toAppend, cell.Str...)
	default:
		panic("invalid cell type")
	}
}

var ErrDataLen = errors.New("expect more data")

func (cell *Cell) DecodeVal(data []byte) (rest []byte, err error) {
	switch cell.Type {
	case TypeI64:
		if len(data) < 8 {
			return data, ErrDataLen
		}

		cell.I64 = int64(binary.LittleEndian.Uint64(data[0:8]))
		return data[8:], nil
	case TypeStr:
		if len(data) < 4 {
			return data, ErrDataLen
		}

		size := int(binary.LittleEndian.Uint32(data[0:4]))
		if len(data) < 4+size {
			return data, ErrDataLen
		}

		cell.Str = slices.Clone(data[4 : 4+size])
		return data[4+size:], nil
	default:
		panic("invalid cell type")
	}
}

func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, ch := range input {
		if ch == 0x00 || ch == 0x01 {
			toAppend = append(toAppend, 0x01, ch+1)
		} else {
			toAppend = append(toAppend, ch)
		}
	}
	return append(toAppend, 0x00)
}

func decodeStrKey(data []byte) (rest, str []byte, err error) {
	idx := slices.Index(data, 0x00)
	if idx == -1 {
		return data, nil, errors.New("string is not ended")
	}

	str = make([]byte, 0, idx)
	rest = data[idx+1:]

	need := data[:idx]
	for i := 0; i < len(need); i++ {
		if need[i] != 0x01 {
			str = append(str, need[i])
			continue
		}

		if i+1 < len(need) {
			switch need[i+1] {
			case 0x01:
				str = append(str, 0x00)
			case 0x02:
				str = append(str, 0x01)
			}
			i++
		} else {
			str = append(str, need[i])
		}

	}

	return rest, str, nil
}

func (cell *Cell) EncodeKey(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.BigEndian.AppendUint64(toAppend, uint64(cell.I64)^(1<<63))
	case TypeStr:
		return encodeStrKey(toAppend, cell.Str)
	default:
		panic("invalid cell type")
	}
}

func (cell *Cell) DecodeKey(data []byte) (rest []byte, err error) {
	switch cell.Type {
	case TypeI64:
		if len(data) < 8 {
			return data, errors.New("expect more data")
		}
		cell.I64 = int64(binary.BigEndian.Uint64(data[0:8]) ^ (1 << 63))
		return data[8:], nil
	case TypeStr:
		rest, cell.Str, err = decodeStrKey(data)
		return rest, err
	default:
		panic("invalid cell type")
	}
}

func CellToStr(cell *Cell) string {
	switch cell.Type {
	case TypeI64:
		return strconv.FormatInt(cell.I64, 10)
	case TypeStr:
		return string(cell.Str)
	default:
		panic("unreachable")
	}
}
