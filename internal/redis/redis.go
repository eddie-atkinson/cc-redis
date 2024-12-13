package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"fmt"
	"io"
	"log/slog"
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
	WAIT     = "wait"
	TYPE     = "type"
	XADD     = "xadd"
	XRANGE   = "xrange"
	XREAD    = "xread"
	INCR     = "incr"
	MULTI    = "multi"
)

type Redis struct {
	store              kvstore.KVStore
	configuration      configurationOptions
	listener           net.Listener
	replicas           map[string]RedisConnection
	processedByteCount int
	ackChan            chan ReplicaAck
}

func NewRedisWithConfig() (Redis, error) {
	config, err := ParseConfigurationFromFlags()

	redis := Redis{
		store:         kvstore.NewKVStore(),
		configuration: config,
		replicas:      map[string]RedisConnection{},
		ackChan:       make(chan ReplicaAck),
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

func (r *Redis) handleConnection(c net.Conn) {
	connection := NewRedisConnection(c)
	defer connection.Close()
	for {
		ctx := context.Background()

		err := connection.WithReadMutex(func() error {
			value, err := connection.Read()

			if err != nil {
				return err
			}

			cmd, response := r.executeCommand(ctx, value, connection)

			if isWriteCommand(cmd) {
				for _, replica := range r.replicas {
					replica.WithWriteMutex(func() error {
						return replica.Send([]serde.Value{value})
					})
				}
				r.processedByteCount += len(value.Marshal())
			}

			err = connection.WithWriteMutex(func() error { return connection.Send(response) })
			return err
		})

		if err != nil {
			if err == io.EOF {
				return
			} else {
				slog.Error(fmt.Sprintf("Error reading from the client %v", err))
				return
			}
		}

	}
}

func (r *Redis) executeCommand(ctx context.Context, value serde.Value, connection RedisConnection) (string, []serde.Value) {
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
		return REPLCONF, r.replconf(commandArray[1:], connection)
	case PSYNC:
		return PSYNC, r.psync(connection)
	case WAIT:
		return WAIT, r.wait(commandArray[1:])
	case TYPE:
		return TYPE, r.typeCmd(ctx, commandArray[1:])
	case XADD:
		return XADD, r.xadd(ctx, commandArray[1:])
	case XRANGE:
		return XRANGE, r.xrange(ctx, commandArray[1:])
	case XREAD:
		return XREAD, r.xread(ctx, commandArray[1:])
	case INCR:
		return INCR, r.incr(ctx, commandArray[1:])
	case MULTI:
		return MULTI, r.multi(ctx, commandArray[1:])
	default:
		return "", []serde.Value{serde.NewError(fmt.Sprintf("invalid command %s", commands))}
	}
}
