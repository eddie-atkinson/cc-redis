package serde

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	ARRAY   = '*'
	BULK    = '$'

	CRLF = "\r\n"
)

type Value interface {
	Marshal() []byte
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)

	if err != nil {
		return err
	}

	return nil
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, 0, err
	}
	return int(i64), n, nil
}

func (r *Resp) readArray() (Array, error) {
	value := Array{make([]Value, 0)}

	len, _, err := r.readInteger()
	if err != nil {
		return value, err
	}

	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return value, err
		}

		value.Items = append(value.Items, val)
	}
	return value, nil
}

func (r *Resp) readBulk() (Value, error) {

	len, _, err := r.readInteger()

	if err != nil {
		return NewBulkString(""), err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	// Trim off the remaining \r\n
	// TODO: really need to deal with non-terminating sequences here
	r.readLine()

	return NewBulkString(string(bulk)), nil
}

func (r *Resp) readSimpleString() (Value, error) {
	value, _, err := r.readLine()

	if err != nil {
		return SimpleString{""}, err
	}

	return SimpleString{string(value[:])}, nil

}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Array{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case STRING:
		return r.readSimpleString()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Array{}, nil
	}
}
