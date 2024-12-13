package redis

import (
	"codecrafters/internal/serde"
	"context"
)

func (r Redis) multi(ctx context.Context, args []string) []serde.Value {
	return []serde.Value{serde.NewSimpleString("OK")}
}
