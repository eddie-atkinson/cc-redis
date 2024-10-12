package redis

import (
	"codecrafters/internal/serde"
)

func (r Redis) psync() serde.Value {
	return serde.NewSimpleString("OK")
}
