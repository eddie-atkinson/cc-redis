package redis

import (
	"codecrafters/internal/serde"
	"fmt"
)

func (r Redis) psync() serde.Value {
	return serde.NewSimpleString(fmt.Sprintf("FULLRESYNC %s 0", r.configuration.replicationConfig.masterReplId))
}
