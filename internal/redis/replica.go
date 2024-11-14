package redis

import "codecrafters/internal/serde"

type Replica struct {
	writer              serde.Writer
	reader              *serde.Reader
	processedBytesCount int
}

func (r Replica) IsUpToDate(processedBytes int) bool {
	return processedBytes == r.processedBytesCount
}
