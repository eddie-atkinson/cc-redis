package kvstore

import (
	"codecrafters/internal/serde"
	"context"
)

type StoredStream struct {
	value map[string]map[string]string
}

func NewStoredStream(value map[string]map[string]string) StoredStream {
	return StoredStream{value: value}
}

func (ss StoredStream) Type() string {
	return "stream"
}

func (ss StoredStream) Value() serde.Value {
	panic("No idea how to serialise this yet")
}

func (ss StoredStream) IsExpired(ctx context.Context) bool {
	return false
}
