package redis

import (
	"codecrafters/internal/serde"
	"context"
	"fmt"
)

func (r Redis) get(ctx context.Context, args []string) serde.Value {
	if len(args) != 1 {
		return serde.NewError(fmt.Sprintf("Expected a single value argument for GET, received %d args", len(args)))
	}
	key := args[0]

	storedValue, found := r.store.GetKey(ctx, key)

	if !found {
		return serde.NewNull()
	}

	return serde.NewBulkString(storedValue.Value())
}
