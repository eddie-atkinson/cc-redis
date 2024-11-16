package redis

import (
	"codecrafters/internal/serde"
)

const INFINITE_WAIT_TIME = 0

func (r *Redis) wait(args []string) []serde.Value {
	if len(args) != 2 {
		return []serde.Value{serde.NewError("WAIT requires two arguments: <numreplicas> <timeout>")}
	}

	// replicCount, err := strconv.Atoi(args[0])

	// if err != nil {
	// 	return []serde.Value{serde.NewError("Number of replicas for WAIT must be an integer")}
	// }

	// waitTime, err := strconv.Atoi(args[1])

	// if err != nil {
	// 	return []serde.Value{serde.NewError("Number of milliseconds for WAIT must be an integer")}
	// }

	// highWaterMark := r.processedByteCount

	// TODO: Need to setup context or similar with timeout here to sit and wait until all replicas have phoned home

	return []serde.Value{serde.NewInteger(0)}
}
