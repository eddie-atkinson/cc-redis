package redis

import (
	"codecrafters/internal/serde"
	"fmt"
)

func (r Redis) info(_ []string) serde.Value {
	return serde.NewBulkString(fmt.Sprintf("role:%s\r\n", r.Role()))
}
