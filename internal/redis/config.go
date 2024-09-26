package redis

import (
	"codecrafters/internal/serde"
	"fmt"
	"strings"
)

func getConfigProperty(arg string, r Redis) serde.Value {
	response := [2]serde.Value{}

	switch v := strings.ToLower(arg); v {
	case "dir":
		response[0] = serde.NewBulkString(v)
		response[1] = serde.NewBulkString(r.configuration.persistenceDir)
	case "dbfilename":
		response[0] = serde.NewBulkString(v)
		response[1] = serde.NewBulkString(r.configuration.persistenceFileName)
	}
	return serde.NewArray(response[:])
}

func (r Redis) config(args []string) serde.Value {
	if len(args) < 2 {
		return serde.NewError("CONFIG requires at least two arguments")
	}

	switch v := strings.ToLower(args[0]); v {
	case "get":
		return getConfigProperty(args[1], r)
	default:
		return serde.NewError(fmt.Sprintf("Do not recognise config command %s", args[1]))
	}
}
