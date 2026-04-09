package database

import (
	"encoding/binary"
	"errors"
	"slices"
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

func (cell *Cell) Encode(toAppend []byte) []byte {
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

func (cell *Cell) Decode(data []byte) (rest []byte, err error) {
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
