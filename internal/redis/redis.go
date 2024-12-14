package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"errors"
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
	EXEC     = "exec"
	DISCARD  = "discard"
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

func (r *Redis) executeAndMaybePropagate(ctx context.Context, cmd string, args []string, value serde.Value, connection RedisConnection) ([]serde.Value, error) {
	cmd, response := r.executeCommand(ctx, cmd, args, connection)

	if isWriteCommand(cmd) {
		for _, replica := range r.replicas {
			replica.WithWriteMutex(func() error {
				return replica.Send([]serde.Value{value})
			})
		}
		r.processedByteCount += len(value.Marshal())
	}
	return response, nil
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

			cmd, args, err := r.parseCommand(value)

			if err != nil {
				return err
			}

			switch cmd {
			case MULTI:
				{
					if connection.transaction {
						return connection.WithWriteMutex(func() error {
							return connection.Send([]serde.Value{serde.NewError("ERR MULTI calls can not be nested")})
						})
					}
					connection.transaction = true
					return connection.Send(r.multi(ctx, []string{}))
				}
			case DISCARD:
				{
					if connection.transaction {
						connection.transaction = false
						connection.bufferedCommands = []serde.Value{}

						return connection.WithWriteMutex(func() error {
							return connection.Send([]serde.Value{serde.NewSimpleString("OK")})
						})
					}
					return connection.WithWriteMutex(func() error {
						return connection.Send([]serde.Value{serde.NewError("ERR DISCARD without MULTI")})
					})
				}
			case EXEC:
				{
					if !connection.transaction {
						return connection.WithWriteMutex(func() error {
							return connection.Send([]serde.Value{serde.NewError("ERR EXEC without MULTI")})
						})
					}
					response := r.exec(ctx, args, connection)
					connection.transaction = false
					err = connection.WithWriteMutex(func() error { return connection.Send(response) })
					return err
				}
			default:
				{
					if connection.transaction {
						connection.bufferedCommands = append(connection.bufferedCommands, value)
						return connection.WithWriteMutex(func() error { return connection.Send([]serde.Value{serde.NewSimpleString("QUEUED")}) })
					}
					response, err := r.executeAndMaybePropagate(ctx, cmd, args, value, connection)
					if err != nil {
						return err
					}
					return connection.WithWriteMutex(func() error { return connection.Send(response) })
				}
			}

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

func (r *Redis) parseCommand(value serde.Value) (string, []string, error) {
	commands, ok := value.(serde.Array)

	if !ok {
		return "", []string{}, errors.New("expected commands to be array")

	}

	commandArray, err := commands.ToCommandArray()

	if err != nil {
		return "", []string{}, err
	}

	if len(commandArray) == 0 {
		return "", []string{}, errors.New("empty commands array")

	}

	cmd := strings.ToLower(commandArray[0])

	return cmd, commandArray[1:], nil
}

func (r *Redis) executeCommand(ctx context.Context, cmd string, commandArray []string, connection RedisConnection) (string, []serde.Value) {

	switch cmd {
	case PING:
		return PING, r.ping()
	case ECHO:
		return ECHO, r.echo(commandArray)
	case SET:
		return SET, r.set(ctx, commandArray)
	case GET:
		return GET, r.get(ctx, commandArray)
	case CONFIG:
		return CONFIG, r.config(commandArray)
	case KEYS:
		return KEYS, r.keys(ctx, commandArray)
	case INFO:
		return INFO, r.info(commandArray)
	case REPLCONF:
		return REPLCONF, r.replconf(commandArray, connection)
	case PSYNC:
		return PSYNC, r.psync(connection)
	case WAIT:
		return WAIT, r.wait(commandArray)
	case TYPE:
		return TYPE, r.typeCmd(ctx, commandArray)
	case XADD:
		return XADD, r.xadd(ctx, commandArray)
	case XRANGE:
		return XRANGE, r.xrange(ctx, commandArray)
	case XREAD:
		return XREAD, r.xread(ctx, commandArray)
	case INCR:
		return INCR, r.incr(ctx, commandArray)
	case MULTI:
		return MULTI, r.multi(ctx, commandArray)
	default:
		return "", []serde.Value{serde.NewError(fmt.Sprintf("invalid command %s %v", cmd, commandArray))}
	}
}
