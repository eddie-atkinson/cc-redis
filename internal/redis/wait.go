package redis

import (
	"codecrafters/internal/serde"
	"log/slog"
	"strconv"
	"time"
)

func (r *Redis) wait(args []string) []serde.Value {
	if len(args) != 2 {
		return []serde.Value{serde.NewError("WAIT requires two arguments: <numreplicas> <timeout>")}
	}

	replicasNeeded, err := strconv.Atoi(args[0])

	if err != nil {
		return []serde.Value{serde.NewError("Number of replicas for WAIT must be an integer")}
	}

	timeoutMs, err := strconv.Atoi(args[1])

	if err != nil {
		return []serde.Value{serde.NewError("Number of milliseconds for WAIT must be an integer")}
	}

	bytesNeeded := r.processedByteCount

	caughtUp := map[string]RedisConnection{}
	for _, replica := range r.replicas {
		// If we already know they're up to date, don't waste time
		if replica.processedByteCount >= bytesNeeded {
			caughtUp[replica.id] = replica
			continue
		}

		go func() {
			err := replica.ReplConfGetAck()
			if err != nil {
				slog.Error("Error getting replication ack from replica:", err)
			}
		}()
	}

ReplicaWaitLoop:
	for len(caughtUp) < replicasNeeded {
		select {
		case ack := <-r.ackChan:
			{
				if ack.processedByteCount >= bytesNeeded {
					caughtUp[ack.connectionId] = r.replicas[ack.connectionId]
				}
			}
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			{
				break ReplicaWaitLoop
			}
		}
	}

	return []serde.Value{serde.NewInteger(int64(len(caughtUp)))}
}
