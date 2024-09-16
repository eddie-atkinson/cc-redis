package serde

import "strconv"

type Array struct {
	items []Value
}

func (a Array) Marshal() []byte {
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, []byte(strconv.Itoa(len(a.items)))...)
	bytes = append(bytes, []byte(CRLF)...)

	for _, v := range a.items {
		bytes = append(bytes, v.Marshal()...)
	}
	return bytes
}
