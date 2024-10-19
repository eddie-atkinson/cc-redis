package redis

import (
	"codecrafters/internal/serde"
)

func (r Redis) replconf(args []string) []serde.Value {

	if len(args) == 0 {
		return []serde.Value{serde.NewError("REPLCONF needs at least one arg")}
	}

	switch args[0] {
	case "ACK":
		{
			if args[1] != "*" {
				return []serde.Value{serde.NewError("expected REPLCONF ACK *")}
			}
			return []serde.Value{serde.NewArray([]serde.Value{
				serde.NewBulkString("REPLCONF"),
				serde.NewBulkString("ACK"),
				serde.NewBulkString("0"),
			})}
		}
	default:
		{
			return []serde.Value{serde.NewSimpleString("OK")}
		}
	}
}
