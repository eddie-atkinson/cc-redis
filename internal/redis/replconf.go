package redis

import (
	"codecrafters/internal/serde"
	"fmt"
	"strconv"
)

func (r Redis) replconf(args []string, connection RedisConnection) []serde.Value {

	if len(args) < 2 {
		return []serde.Value{serde.NewError("REPLCONF needs at least one arg")}
	}

	switch args[0] {
	case "GETACK":
		{
			if args[1] != "*" {
				return []serde.Value{serde.NewError("expected REPLCONF GETACK *")}
			}
			return []serde.Value{serde.NewArray([]serde.Value{
				serde.NewBulkString("REPLCONF"),
				serde.NewBulkString("ACK"),
				serde.NewBulkString(fmt.Sprintf("%d", r.processedByteCount)),
			})}
		}
	case "ACK":
		{
			processedBytes, err := strconv.Atoi(args[1])
			if err != nil {
				return []serde.Value{serde.NewError("REPLCONF ACK takes a number for offset")}
			}
			// TODO: probably needs a lock?
			connection.processedByteCount = processedBytes
			r.ackChan <- ReplicaAck{connectionId: connection.id, processedByteCount: processedBytes}
			return []serde.Value{}
		}
	default:
		{
			return []serde.Value{serde.NewSimpleString("OK")}
		}
	}
}
