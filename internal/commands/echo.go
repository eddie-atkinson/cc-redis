package commands

import (
	"codecrafters/internal/serde"
	"fmt"
)

func echo(arguments []serde.Value) serde.Value {
	if len(arguments) != 1 {
		return serde.Error{Value: fmt.Sprintf("ECHO expects %d argument, received: %d", 1, len(arguments))}
	}

	echoValue := arguments[0]

	switch v := echoValue.(type) {
	case serde.BulkString:
		return v
	default:
		return serde.Error{Value: "Expected bulk string argument to ECHO"}
	}
}
