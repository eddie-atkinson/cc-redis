package commands

import (
	"codecrafters/internal/serde"
	"fmt"
	"strings"
)

var handlers = map[string]func([]serde.Value) serde.Value{
	"ping": ping,
	"echo": echo,
}

func ExecuteCommand(command serde.Array) serde.Value {
	if len(command.Items) == 0 {
		return serde.Error{Value: "Empty commands array"}
	}

	switch v := command.Items[0].(type) {
	case serde.BulkString:
		handler, ok := handlers[strings.ToLower(v.Value)]
		if !ok {
			return serde.Error{Value: fmt.Sprintf("Invalid command %s", command)}
		}
		return handler(command.Items[1:])
	default:
		return serde.Error{Value: "Expected command to bulk string"}
	}

}
