package redis

import (
	"codecrafters/internal/serde"
	"context"
	"fmt"
)

func parseXaddArgs(args []string) (map[string]string, error) {

	parsedArgs := map[string]string{}

	if len(args)%2 != 0 {
		return parsedArgs, fmt.Errorf("args to XADD must be key value pairs %v", args)
	}

	for i := 0; i < len(args); i = i + 2 {
		key := args[i]
		value := args[i+1]

		parsedArgs[key] = value
	}

	return parsedArgs, nil
}

func (r Redis) xadd(ctx context.Context, args []string) []serde.Value {
	if len(args) < 4 {
		return []serde.Value{serde.NewError("XADD expects at least four arguments")}
	}

	key := args[0]
	id := args[1]

	parsedArgs, err := parseXaddArgs(args[2:])

	if err != nil {
		return []serde.Value{serde.NewError(err.Error())}
	}

	if err != nil {
		return []serde.Value{serde.NewError(err.Error())}
	}

	insertedId, _, err := r.store.SetStream(ctx, key, id, parsedArgs)

	if err != nil {
		return []serde.Value{serde.NewError(err.Error())}
	}

	return []serde.Value{serde.NewBulkString(insertedId.ToString())}
}
