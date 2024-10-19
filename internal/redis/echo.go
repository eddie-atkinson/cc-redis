package redis

import (
	"codecrafters/internal/serde"
	"fmt"
)

func (r Redis) echo(arguments []string) []serde.Value {
	if len(arguments) != 1 {
		return []serde.Value{serde.NewError(fmt.Sprintf("ECHO expects %d argument, received: %d", 1, len(arguments)))}
	}

	return []serde.Value{serde.NewBulkString(arguments[0])}
}
