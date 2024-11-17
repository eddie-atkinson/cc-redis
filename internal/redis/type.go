package redis

import (
	"codecrafters/internal/serde"
	"context"
)

const (
	NONE   = "none"
	STRING = "string"
)

func (r *Redis) typeCmd(ctx context.Context, args []string) []serde.Value {
	if len(args) != 1 {
		return []serde.Value{serde.NewError("TYPE requires a key argument")}
	}

	key := args[0]

	stored, found := r.store.GetKey(ctx, key)

	if !found {
		return []serde.Value{serde.NewSimpleString(NONE)}
	}

	return []serde.Value{serde.NewSimpleString(stored.Type())}
}
