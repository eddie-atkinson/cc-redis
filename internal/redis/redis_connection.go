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

func (r RedisConnection) Ping() error {
	err := r.Send([]serde.Value{serde.NewArray([]serde.Value{serde.NewBulkString("PING")})})

	if err != nil {
		return err
	}

	response, err := r.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || strings.ToLower(simpleString.Value()) != "pong" {
		return fmt.Errorf("expected ping to respond with 'PONG' got %v", response)
	}

	return nil
}

func (r RedisConnection) ReplConf(args []string) error {
	command := append([]string{"REPLCONF"}, args...)

	// TODO: Ask the tie dye man why this can't be serde.BulkString
	commandArr := array.Map(command, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})

	err := r.Send([]serde.Value{serde.NewArray(commandArr)})

	if err != nil {
		return err
	}
	response, err := r.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || strings.ToLower(simpleString.Value()) != "ok" {
		return fmt.Errorf("expected ping to respond with 'OK' got %v", response)
	}

	return err
}

func (r RedisConnection) Psync(replicationId string, offset string) error {
	command := array.Map([]string{"PSYNC", replicationId, offset}, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})

	err := r.Send([]serde.Value{serde.NewArray(command)})

	if err != nil {
		return err
	}

	response, err := r.Read()

	if err != nil {
		return err
	}

	simpleString, ok := response.(serde.SimpleString)

	if !ok || !strings.HasPrefix(simpleString.Value(), "FULLRESYNC") {
		return fmt.Errorf("expected to receive full sync on child, got %s instead", simpleString.Value())
	}

	return r.ReadRDB()
}

func (r RedisConnection) ReplConfGetAck(connectionId string, ackChan chan<- ReplicaAck) error {

	command := []string{"REPLCONF", "GETACK", "*"}

	// TODO: Ask the tie dye man why this can't be serde.BulkString
	commandArr := array.Map(command, func(s string) serde.Value {
		return serde.NewBulkString(s)
	})
	err := r.Send([]serde.Value{serde.NewArray(commandArr)})

	if err != nil {
		return err
	}

	response, err := r.Read()

	if err != nil {
		return err
	}

	array, ok := response.(serde.Array)

	if !ok || len(array.Items) != 3 {
		return fmt.Errorf("expected array of length 3 in response to replconf get ack %v", response)
	}
	return nil

	// offsetItem, ok := array.Items[2].(serde.BulkString)

	// if !ok {
	// 	return fmt.Errorf("expected replconf get ack to respond with offset, instead received %v", array.Items[2])
	// }

	// offset, err := strconv.Atoi(offsetItem.Value())

	// if err != nil {
	// 	return err
	// }

	// ackChan <- ReplicaAck{connectionId: connectionId, processedByteCount: offset}

	// return nil
}

func (r RedisConnection) Send(value []serde.Value) error {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	var err error

	for _, v := range value {
		err := r.writer.Write(v)
		if err != nil {
			return err
		}
	}

	return err
}

func (r RedisConnection) Read() (serde.Value, error) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	return r.reader.Read()
}

func (r RedisConnection) ReadRDB() error {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	return r.reader.ReadRDB()
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
