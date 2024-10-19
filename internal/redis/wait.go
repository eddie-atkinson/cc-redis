package redis

import (
	"codecrafters/internal/serde"
)

func (r *Redis) wait(_ []string) []serde.Value {
	return []serde.Value{serde.NewInteger(0)}
}
