package kvstore

import (
	"codecrafters/internal/serde"
	"codecrafters/internal/time"
	"context"
)

type StoredString struct {
	value     string
	expiresAt *uint64
}

func NewStoredString(value string, expiresAt *uint64) StoredString {
	return StoredString{value: value, expiresAt: expiresAt}
}

func (ss StoredString) IsExpired(ctx context.Context) bool {
	return time.HasExpired(ctx, ss.expiresAt)
}

func (ss StoredString) Type() string {
	return "string"
}

func (ss StoredString) Value() serde.Value {
	return serde.NewBulkString(ss.value)
}
