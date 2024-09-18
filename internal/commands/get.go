package commands

import (
	"codecrafters/internal/serde"
	"fmt"
)

func get(args []string) serde.Value {
	if len(args) != 1 {
		return serde.NewError(fmt.Sprintf("Expected a single value argument for GET, received %d args", len(args)))
	}
	key := args[0]

	storedValue, found := store.getKey(key)

	if !found {
		return serde.NewNull()
	}

	return serde.NewBulkString(storedValue.value)
}
