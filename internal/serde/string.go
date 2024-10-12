package serde

import "strconv"

type SimpleString struct {
	value string
}

func (s SimpleString) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, s.value[:]...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}

func (s SimpleString) Value() string {
	return s.value
}

type BulkString struct {
	value string
}

func (bs BulkString) Equal(other BulkString) bool {
	return bs.Value() == other.Value()
}

func (bs BulkString) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, []byte(strconv.Itoa(len(bs.value)))...)
	bytes = append(bytes, []byte(CRLF)...)
	bytes = append(bytes, []byte(bs.value)...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}

func (s BulkString) Value() string {
	return s.value
}

func Ok() SimpleString {
	return SimpleString{"OK"}
}

func NewSimpleString(value string) SimpleString {
	return SimpleString{value}
}

func NewBulkString(value string) BulkString {
	return BulkString{value}
}
