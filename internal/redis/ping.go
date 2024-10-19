package redis

import (
	"codecrafters/internal/serde"
)

func (r Redis) ping() []serde.Value {
	return []serde.Value{serde.NewSimpleString("PONG")}
}
