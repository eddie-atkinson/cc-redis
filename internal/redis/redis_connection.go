package redis

import (
	"codecrafters/internal/array"
	"codecrafters/internal/serde"
	"fmt"
	"net"
	"strings"
	"sync"
)

type RedisConnection struct {
	reader    *serde.Reader
	writer    serde.Writer
	conn      net.Conn
	connMutex *sync.RWMutex
}

func (rc RedisConnection) Ping() error {
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

func (rc RedisConnection) ReplConf(args []string) error {
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

	return err
}

func (rc RedisConnection) Psync(replicationId string, offset string) error {
	command := array.Map([]string{"PSYNC", replicationId, offset}, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})

	err := rc.writer.Write(serde.NewArray(command))

	if err != nil {
		return err
	}

	response, err := rc.reader.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || !strings.HasPrefix(simpleString.Value(), "FULLRESYNC") {
		return fmt.Errorf("expected to receive full sync on child, got %s instead", simpleString.Value())
	}

	return rc.reader.ReadRDB()
}

func (r RedisConnection) Send(value []serde.Value) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	for _, v := range value {
		r.writer.Write(v)
	}
}

func (r RedisConnection) Read() (serde.Value, error) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	return r.reader.Read()
}

func (r RedisConnection) Close() {
	r.conn.Close()
}

func NewRedisConnection(c net.Conn) RedisConnection {
	reader := serde.NewReader(c)
	writer := serde.NewWriter(c)

	return RedisConnection{
		reader:    &reader,
		writer:    writer,
		conn:      c,
		connMutex: &sync.RWMutex{},
	}
}
