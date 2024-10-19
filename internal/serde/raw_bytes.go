package serde

type RawBytes struct {
	value []byte
}

func (rb RawBytes) Marshal() []byte {
	return rb.value
}

func NewRawBytes(b []byte) RawBytes {
	return RawBytes{value: b}
}
