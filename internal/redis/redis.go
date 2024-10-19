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

const (
	PING     = "ping"
	SET      = "set"
	INFO     = "info"
	ECHO     = "echo"
	GET      = "get"
	CONFIG   = "config"
	KEYS     = "keys"
	REPLCONF = "replconf"
	PSYNC    = "psync"
)

type Redis struct {
	store         kvstore.KVStore
	configuration configurationOptions
	listener      net.Listener
	replicas      []serde.Writer
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

	port := fmt.Sprintf(":%d", config.port)
	fmt.Println("Listening on ", port)
	listener, err := net.Listen("tcp", port)

	if err != nil {
		return redis, err
	}

	redis.listener = listener

	return redis, nil
}

func (r Redis) Port() int {
	return r.configuration.port
}

func (r *Redis) Init() error {
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

func isWriteCommand(cmd string) bool {
	switch cmd {
	case SET:
		return true
	default:
		return false
	}
}

func (r Redis) isMasterNode() bool {
	return r.configuration.replicationConfig.replicaConfig.Role() == MASTER
}

func writeResponse(writer serde.Writer, response []serde.Value) {
	for _, v := range response {
		writer.Write(v)
	}
}

func (r *Redis) handleConnection(c net.Conn) {
	defer c.Close()
	for {
		reader := serde.NewReader(c)
		writer := serde.NewWriter(c)
		ctx := context.Background()

		value, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from the client: ", err.Error())
			return
		}

		cmd, response := r.executeCommand(ctx, value, writer)

		if r.isMasterNode() {
			writeResponse(writer, response)
			return
		}

		if cmd == INFO {
			writeResponse(writer, response)
		}
	}
}

func (r *Redis) executeCommand(ctx context.Context, value serde.Value, writer serde.Writer) (string, []serde.Value) {
	commands, ok := value.(serde.Array)

	if !ok {
		return "", []serde.Value{serde.NewError("Expected commands to be array")}

	}
	commandArray, err := commands.ToCommandArray()

	if err != nil {
		return "", []serde.Value{serde.NewError(err.Error())}
	}

	if len(commandArray) == 0 {
		return "", []serde.Value{serde.NewError("empty commands array")}

	}

	cmd := strings.ToLower(commandArray[0])

	if isWriteCommand(cmd) {
		for _, v := range r.replicas {
			v.Write(value)
		}
	}

	switch cmd {
	case PING:
		return PING, r.ping()
	case ECHO:
		return ECHO, r.echo(commandArray[1:])
	case SET:
		return SET, r.set(ctx, commandArray[1:])
	case GET:
		return GET, r.get(ctx, commandArray[1:])
	case CONFIG:
		return CONFIG, r.config(commandArray[1:])
	case KEYS:
		return KEYS, r.keys(ctx, commandArray[1:])
	case INFO:
		return INFO, r.info(commandArray[1:])
	case REPLCONF:
		return REPLCONF, r.replconf()
	case PSYNC:
		return PSYNC, r.psync(writer)
	default:
		return "", []serde.Value{serde.NewError(fmt.Sprintf("invalid command %s", commands))}
	}

}
