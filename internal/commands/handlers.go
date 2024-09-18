package commands

import (
	"codecrafters/internal/serde"
	"fmt"
	"strings"
)

var handlers = map[string]func([]string) serde.Value{
	"ping": ping,
	"echo": echo,
	"set":  set,
	"get":  get,
}

func ExecuteCommand(command []string) serde.Value {
	if len(command) == 0 {
		serde.NewError("empty commands array")
	}

	handler, ok := handlers[strings.ToLower(command[0])]
	if !ok {
		serde.NewError(fmt.Sprintf("invalid command %s", command))

	}
	return handler(command[1:])
}
