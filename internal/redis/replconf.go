package redis

import (
	"codecrafters/internal/serde"
)

func (r Redis) replconf() []serde.Value {
	return []serde.Value{serde.NewSimpleString("OK")}
}
