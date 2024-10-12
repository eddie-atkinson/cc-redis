package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
)

type Redis struct {
	store         kvstore.KVStore
	configuration configurationOptions
}

func NewRedisWithConfig() (*Redis, error) {
	config, err := ParseConfigurationFromFlags()
	if err != nil {
		return nil, err
	}
	store := kvstore.NewKVStore()

	return &Redis{store, config}, nil
}

func (r Redis) Port() int {
	return r.configuration.port
}

func (r Redis) Init() error {
	return r.processRDBFile()
}

func (r Redis) HandleConnection(c net.Conn) {
	defer c.Close()
	for {
		resp := serde.NewResp(c)
		writer := serde.NewWriter(c)
		ctx := context.Background()

		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from the client: ", err.Error())
			return
		}
		writer.Write(r.executeCommand(ctx, value))
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
