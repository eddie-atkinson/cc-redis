package redis

import (
	"codecrafters/internal/serde"
	"fmt"
)

func parseXaddArgs(args []string) (map[string]string, error) {

	parsedArgs := map[string]string{}

	if len(args)%2 != 0 {
		return parsedArgs, fmt.Errorf("args to XADD must be key value pairs %v", args)
	}

	// TODO (eatkinson): Support more arguments and clean up the parsing here
	for i := 0; i < len(args); i = i + 2 {
		key := args[i]
		value := args[i+1]

		parsedArgs[key] = value
	}

	return parsedArgs, nil
}

func (r Redis) xadd(args []string) []serde.Value {
	if len(args) < 4 {
		return []serde.Value{serde.NewError("SET expects at least four arguments")}
	}

	key := args[0]
	id := args[1]

	parsedArgs, err := parseXaddArgs(args[2:])

	valueToStore := map[string]map[string]string{
		id: parsedArgs,
	}

	if err != nil {
		return []serde.Value{serde.NewError(err.Error())}
	}

	r.store.SetStream(key, valueToStore)

	return []serde.Value{serde.NewSimpleString(id)}
}
