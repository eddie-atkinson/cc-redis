package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"fmt"
	"strings"
)

type Redis struct {
	store         kvstore.KVStore
	configuration configurationOptions
}

func NewRedisWithConfig() (Redis, error) {
	config, err := ParseConfigurationFromFlags()

	redis := Redis{
		store:         kvstore.NewKVStore(),
		configuration: config,
	}

	if err != nil {
		return redis, err
	}

	return redis, nil
}

func (r Redis) Port() int {
	return r.configuration.port
}

func (r Redis) Init() error {
	err := r.processRDBFile()
	if err != nil {
		return err
	}

	if r.configuration.replicationConfig.replicaConfig.Role() == MASTER {
		return initMaster(r)
	} else {
		return initSlave(r)
	}
}

func (r Redis) executeCommand(ctx context.Context, value serde.Value) serde.Value {
	command, ok := value.(serde.Array)
	if !ok {
		return serde.NewError("Expected commands to be array")
	}
	commandArray, err := command.ToCommandArray()

	if err != nil {
		return serde.NewError(err.Error())
	}

	if len(commandArray) == 0 {
		return serde.NewError("empty commands array")
	}

	switch strings.ToLower(commandArray[0]) {
	case "ping":
		return r.ping()
	case "echo":
		return r.echo(commandArray[1:])
	case "set":
		return r.set(ctx, commandArray[1:])
	case "get":
		return r.get(ctx, commandArray[1:])
	case "config":
		return r.config(commandArray[1:])
	case "keys":
		return r.keys(ctx, commandArray[1:])
	case "info":
		return r.info(commandArray[1:])
	default:
		return serde.NewError(fmt.Sprintf("invalid command %s", command))

	}

}
