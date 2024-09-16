package serde

type Null struct{}

func (null Null) Marshal() []byte {
	return []byte("$-1\r\n")
}
