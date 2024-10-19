package serde

import (
	"fmt"
)

type Integer struct {
	value int64
}

func (i Integer) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, []byte(fmt.Sprintf("%d", i.value))...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}

func NewInteger(value int64) Integer {
	return Integer{value}
}
