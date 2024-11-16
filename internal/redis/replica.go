package redis

type Replica struct {
	connection          RedisConnection
	processedBytesCount int
}

func (r Replica) IsUpToDate(processedBytes int) bool {
	return processedBytes == r.processedBytesCount
}
