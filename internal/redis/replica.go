package redis

import "github.com/dchest/uniuri"

type Replica struct {
	connection         RedisConnection
	processedByteCount int
	id                 string
}

func NewReplica(connection RedisConnection) Replica {
	return Replica{
		connection:         connection,
		processedByteCount: 0,
		id:                 uniuri.NewLen(40),
	}
}

func (r Replica) IsUpToDate(processedBytes int) bool {
	return processedBytes == r.processedByteCount
}
