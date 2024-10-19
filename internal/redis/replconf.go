package redis

import (
	"codecrafters/internal/serde"
	"fmt"
)

func (r Redis) replconf(args []string) []serde.Value {

	if len(args) == 0 {
		return []serde.Value{serde.NewError("REPLCONF needs at least one arg")}
	}

	switch args[0] {
	case "GETACK":
		{
			if args[1] != "*" {
				return []serde.Value{serde.NewError("expected REPLCONF ACK *")}
			}
			return []serde.Value{serde.NewArray([]serde.Value{
				serde.NewBulkString("REPLCONF"),
				serde.NewBulkString("ACK"),
				serde.NewBulkString(fmt.Sprintf("%d", r.processedByteCount)),
			})}
		}
	default:
		{
			return []serde.Value{serde.NewSimpleString("OK")}
		}
	}
}
