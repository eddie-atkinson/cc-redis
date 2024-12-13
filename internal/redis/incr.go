package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"strconv"
)

func (r Redis) incr(ctx context.Context, args []string) []serde.Value {
	if len(args) != 1 {
		return []serde.Value{serde.NewError("INCR requires at least one arg")}
	}
	key := args[0]

	value, exists := r.store.GetKey(ctx, key)

	storedValue := 0

	if exists {
		storedString, ok := value.(kvstore.StoredString)

		if !ok {
			return []serde.Value{serde.NewError("ERR value is not an integer or out of range")}
		}

		parsed, err := strconv.Atoi(storedString.ToString())

		if err != nil {
			return []serde.Value{serde.NewError("ERR value is not an integer or out of range")}
		}

		storedValue = parsed
	}

	storedValue += 1

	r.store.SetKeyWithExpiry(ctx, key, strconv.Itoa(storedValue), nil)

	return []serde.Value{serde.NewInteger(int64(storedValue))}
}
