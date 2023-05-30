package main

import (
	"bytes"
	"fmt"
)

const (
	UnitSeparator byte = 0x1f
)

type Op interface {
	Name() string
	Encode(*bytes.Buffer)
	Decode([]byte) error
}

func EncodeOp(op Op) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString(op.Name())
	buf.WriteByte(0)
	op.Encode(&buf)

	return buf.Bytes(), nil
}

func DecodeOp(data []byte) (Op, error) {
	sep := bytes.IndexByte(data, UnitSeparator)
	if sep == -1 {
		return nil, fmt.Errorf("invalid data")
	}

	var op Op

	name := string(data[:sep])
	switch name {
	case "put":
		op = &OpPut{}
	case "delete":
		op = &OpDelete{}
	default:
		return nil, fmt.Errorf("unknown op %q", name)
	}

	if err := op.Decode(data[sep+1:]); err != nil {
		return nil, err
	}

	return op, nil
}

type OpPut struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (op OpPut) Name() string {
	return "put"
}

func (op OpPut) Encode(buf *bytes.Buffer) {
	buf.WriteString(op.Key)
	buf.WriteByte(UnitSeparator)
	buf.WriteString(op.Value)
}

func (op *OpPut) Decode(data []byte) error {
	sep := bytes.IndexByte(data, UnitSeparator)
	if sep == -1 {
		return fmt.Errorf("invalid data")
	}

	op.Key = string(data[:sep])
	op.Value = string(data[sep+1:])

	return nil
}

type OpDelete struct {
	Key string `json:"key"`
}

func (op OpDelete) Name() string {
	return "delete"
}

func (op OpDelete) Encode(buf *bytes.Buffer) {
	buf.WriteString(op.Key)
}

func (op *OpDelete) Decode(data []byte) error {
	op.Key = string(data)
	return nil
}
