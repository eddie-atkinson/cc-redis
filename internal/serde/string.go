package serde

import "strconv"

type SimpleString struct {
	Value string
}

func (s SimpleString) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, s.Value[:]...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}

type BulkString struct {
	value string
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
