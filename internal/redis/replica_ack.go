package redis

type ReplicaAck struct {
	connectionId       string
	processedByteCount int
}
