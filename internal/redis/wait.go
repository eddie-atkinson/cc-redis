package redis

import (
	"codecrafters/internal/serde"
	"fmt"
	"strconv"
	"time"
)

const INFINITE_WAIT_TIME = 0

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

	fmt.Printf("In master want %v", r.processedByteCount)

	ackChan := make(chan ReplicaAck)
	defer close(ackChan)

	caughtUp := 0
	startTime := time.Now()

	for id, replica := range r.replicas {
		go func(replica Replica) {
			err := replica.connection.ReplConfGetAck(id, ackChan)
			if err != nil {
				fmt.Println("Error getting replication ack from replica:", err)
			}
		}(replica)
	}

	for caughtUp < replicasNeeded && time.Since(startTime) < time.Duration(timeoutMs)*time.Millisecond {
		select {
		case ack, ok := <-ackChan:
			fmt.Printf("received ack %v, ok %v", ack, ok)
			if !ok {
				return []serde.Value{serde.NewError("error receiving replica acknowledgment")}
			}
			replica, ok := r.replicas[ack.connectionId]

			if !ok {
				continue
			}

			replica.processedByteCount = ack.processedByteCount

			if replica.processedByteCount > bytesNeeded {
				caughtUp++
			}

		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return []serde.Value{serde.NewInteger(int64(caughtUp))}
		}
	}

	return []serde.Value{serde.NewInteger(int64(caughtUp))}
}
