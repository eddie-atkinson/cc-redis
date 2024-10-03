package redis

import (
	"codecrafters/internal/serde"
	"context"
)

func (r Redis) keys(ctx context.Context, args []string) serde.Value {
	if len(args) != 1 {
		return serde.NewError("KEYS requires one arg")
	}

	if args[0] != "*" {
		return serde.NewError(("command KEYS currently only supports the * arg"))
	}
	keys := r.store.GetKeys(ctx)

	keysAsString := []serde.Value{}

	for _, k := range keys {
		keysAsString = append(keysAsString, serde.NewSimpleString(k))
	}

	if len(keysAsString) == 0 {
		return serde.NewNull()
	}

	return serde.NewArray(keysAsString)
}
