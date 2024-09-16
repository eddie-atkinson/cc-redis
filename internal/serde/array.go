package serde

import "strconv"

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
