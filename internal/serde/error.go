package serde

type Error struct {
	value string
}

func (s Error) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, s.value[:]...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}

func NewError(value string) Error {
	return Error{value}
}
