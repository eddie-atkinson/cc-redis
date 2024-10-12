package redis

import (
	"codecrafters/internal/serde"
	"fmt"
	"strconv"
	"strings"
)

func getInfoLine(key string, value string) string {
	return fmt.Sprintf("%s:%s", key, value)
}

func getReplicationInfo(r Redis) []string {
	header := "# Replication"
	role := getInfoLine("role", r.configuration.replicationConfig.replicaConfig.Role())
	replicaId := getInfoLine("master_replid", r.configuration.replicationConfig.masterReplId)
	replicaOffset := getInfoLine("master_repl_offset", strconv.Itoa(r.configuration.replicationConfig.masterReplOffset))
	replicationInfo := []string{header, role, replicaId, replicaOffset}

	return replicationInfo
}

func (r Redis) info(_ []string) serde.Value {
	replicationInfo := getReplicationInfo(r)
	return serde.NewBulkString(fmt.Sprintf("%s\r\n", strings.Join(replicationInfo, "\r\n")))
}
