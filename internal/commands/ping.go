package commands

import "codecrafters/internal/serde"

func ping(_ []serde.Value) serde.Value {
	return serde.NewSimpleString("PONG")
}
