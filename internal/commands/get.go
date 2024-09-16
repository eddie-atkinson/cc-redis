package commands

import (
	"codecrafters/internal/serde"
	"fmt"
)

func get(args []serde.Value) serde.Value {
	if len(args) != 1 {
		return serde.NewError(fmt.Sprintf("Expected a single value argument for GET, received %d args", len(args)))
	}
	key, ok := args[0].(serde.BulkString)

	if !ok {
		return serde.NewError("GET key must be a bulk string")
	}

	storeMutex.RLock()
	value, ok := store[key.Value()]
	storeMutex.RUnlock()

	if !ok {
		return serde.NewNull()
	}

	return serde.NewBulkString(value)
}
