package serde

type Error struct {
	Value string
}

func (s Error) Marshal() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, s.Value[:]...)
	bytes = append(bytes, []byte(CRLF)...)
	return bytes
}
