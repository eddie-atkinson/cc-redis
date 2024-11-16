package redis

import (
	"codecrafters/internal/serde"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	CONNECTION_TYPE = "tcp"
)

type RedisClient struct {
	reader serde.Reader
	writer serde.Writer
}

func NewRedisClient(conn *net.TCPConn) RedisClient {
	reader := serde.NewReader(conn)
	writer := serde.NewWriter(conn)

	return RedisClient{
		reader,
		writer,
	}
}

func handleSlaveReplicationConnection(r *Redis, connection RedisConnection) {
	defer connection.Close()
	for {

		ctx := context.Background()

		value, err := connection.reader.Read()

		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from the client: ", err.Error())
			return
		}

		cmd, response := r.executeCommand(ctx, value, connection)
		r.processedByteCount += len(value.Marshal())

		if cmd == REPLCONF {
			connection.Send(response)
		}
	}
}

func initSlave(r *Redis) error {
	hostConfig, ok := r.configuration.replicationConfig.replicaConfig.(slaveConfig)

	if !ok {
		return errors.New("expected slave to have replica config")
	}

	server, err := net.ResolveTCPAddr(CONNECTION_TYPE, fmt.Sprintf("%s:%d", hostConfig.host, hostConfig.port))

	if err != nil {
		return err
	}

	conn, err := net.DialTCP(CONNECTION_TYPE, nil, server)

	if err != nil {
		return err
	}

	connection := NewRedisConnection(conn)

	err = connection.Ping()

	if err != nil {
		return err
	}

	err = connection.ReplConf([]string{
		"listening-port",
		fmt.Sprintf("%d", r.configuration.port),
	})

	if err != nil {
		return err
	}

	err = connection.ReplConf([]string{
		"capa",
		"psync2",
	})

	if err != nil {
		return err
	}

	err = connection.Psync("?", "-1")

	if err != nil {
		return err
	}
	go handleSlaveReplicationConnection(r, connection)

	// TODO(eatkinson): We're not a master node this is weird, but should get the tests to pass
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go r.handleConnection(conn)
	}

}
