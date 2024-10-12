package redis

import (
	"codecrafters/internal/array"
	"codecrafters/internal/serde"
	"errors"
	"fmt"
	"net"
	"strings"
)

const (
	CONNECTION_TYPE = "tcp"
)

type RedisClient struct {
	reader serde.Reader
	writer serde.Writer
}

func (rc RedisClient) Ping() error {
	err := rc.writer.Write(serde.NewArray([]serde.Value{serde.NewBulkString("PING")}))
	if err != nil {
		return err
	}

	response, err := rc.reader.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || strings.ToLower(simpleString.Value()) != "pong" {
		return fmt.Errorf("expected ping to respond with 'PONG' got %v", response)
	}

	return nil
}

func (rc RedisClient) ReplConf(args []string) error {
	command := append([]string{"REPLCONF"}, args...)

	// TODO: Ask the tie dye man why this can't be serde.BulkString
	commandArr := array.Map(command, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})

	err := rc.writer.Write(serde.NewArray(commandArr))

	if err != nil {
		return err
	}
	response, err := rc.reader.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || strings.ToLower(simpleString.Value()) != "ok" {
		return fmt.Errorf("expected ping to respond with 'OK' got %v", response)
	}

	return nil
}

func (rc RedisClient) Psync(replicationId string, offset string) error {
	command := array.Map([]string{"PSYNC", replicationId, offset}, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})

	err := rc.writer.Write(serde.NewArray(command))

	return err
}

func NewRedisClient(conn *net.TCPConn) RedisClient {
	reader := serde.NewReader(conn)
	writer := serde.NewWriter(conn)

	return RedisClient{
		reader,
		writer,
	}
}
func initSlave(r Redis) error {
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

	defer conn.Close()

	redisClient := NewRedisClient(conn)

	err = redisClient.Ping()

	if err != nil {
		return err
	}

	err = redisClient.ReplConf([]string{
		"listening-port",
		fmt.Sprintf("%d", r.configuration.port),
	})

	if err != nil {
		return err
	}

	err = redisClient.ReplConf([]string{
		"capa",
		"psync2",
	})

	if err != nil {
		return err
	}

	err = redisClient.Psync("?", "-1")

	if err != nil {
		return err
	}

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