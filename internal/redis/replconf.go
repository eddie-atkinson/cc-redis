package redis

import (
	"codecrafters/internal/serde"
	"fmt"
	"log/slog"
	"strconv"
)

func (r Redis) replconf(args []string, connection RedisConnection) []serde.Value {

	if len(args) < 2 {
		return []serde.Value{serde.NewError("REPLCONF needs at least one arg")}
	}

	switch args[0] {
	case "GETACK":
		{
			slog.Debug(fmt.Sprintf("Slave received GETACK with command %v", args))
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
			slog.Info(fmt.Sprintf("Master received ACK back from slave %v with %v as processedBytes. Master currently at %v bytes", connection.id, processedBytes, r.processedByteCount))
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
