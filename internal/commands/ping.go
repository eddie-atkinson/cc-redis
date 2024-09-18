package commands

import "codecrafters/internal/serde"

func ping(_ []string) serde.Value {
	return serde.NewSimpleString("PONG")
}
