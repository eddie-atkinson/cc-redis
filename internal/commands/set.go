package commands

import (
	"codecrafters/internal/serde"
	"fmt"
	"sync"
)

var store = map[string]string{}
var storeMutex = sync.RWMutex{}

func set(args []serde.Value) serde.Value {
	// TODO (eatkinson): This is a simplification that I'm happy with for now.
	// We also need to support expiry and other flags eventually
	if len(args) != 2 {
		return serde.NewError(fmt.Sprintf("Expected 2 arguments to SET, got %d", len(args)))
	}

	key, keyIsBulkString := args[0].(serde.BulkString)
	value, valueIsBulkString := args[1].(serde.BulkString)

	if !keyIsBulkString || !valueIsBulkString {
		return serde.NewError("SET command requires both key and value to be bulk strings")
	}

	storeMutex.Lock()
	store[key.Value()] = value.Value()
	storeMutex.Unlock()

	return serde.Ok()
}
