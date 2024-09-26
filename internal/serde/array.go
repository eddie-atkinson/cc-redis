package serde

import (
	"errors"
	"strconv"
)

type Array struct {
	Items []Value
}

func (a Array) Marshal() []byte {
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, []byte(strconv.Itoa(len(a.Items)))...)
	bytes = append(bytes, []byte(CRLF)...)

	for _, v := range a.Items {
		bytes = append(bytes, v.Marshal()...)
	}
	return bytes
}

func (a Array) ToCommandArray() ([]string, error) {
	stringArr := []string{}

	for _, value := range a.Items {
		switch valueType := value.(type) {
		case BulkString:
			stringArr = append(stringArr, valueType.Value())
		default:
			return stringArr, errors.New("commands array must be solely composed of bulk string")
		}
	}
	return stringArr, nil
}

func NewArray(items []Value) Array {
	return Array{Items: items}
}
