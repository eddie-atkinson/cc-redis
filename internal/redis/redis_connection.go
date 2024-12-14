package redis

import (
	"codecrafters/internal/array"
	"codecrafters/internal/serde"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/dchest/uniuri"
)

type RedisConnection struct {
	reader             *serde.Reader
	writer             serde.Writer
	conn               net.Conn
	readMutex          *sync.Mutex
	writeMutex         *sync.Mutex
	id                 string
	processedByteCount int
	transaction        bool
	bufferedCommands   []serde.Value
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

func (r RedisConnection) ReplConfGetAck() error {
	return r.WithWriteMutex(func() error {
		command := []string{"REPLCONF", "GETACK", "*"}

		commandArr := array.Map(command, func(s string) serde.Value {
			return serde.NewBulkString(s)
		})
		err := r.Send([]serde.Value{serde.NewArray(commandArr)})

		return err
	})
}

func (r RedisConnection) Send(value []serde.Value) error {

	var err error

	for _, v := range value {
		err := r.writer.Write(v)
		if err != nil {
			return err
		}
	}

	slog.Debug(fmt.Sprintf("Sent %v", value))
	return err
}

func (r RedisConnection) WithReadMutex(f func() error) error {
	r.readMutex.Lock()
	defer r.readMutex.Unlock()
	return f()
}

func (r RedisConnection) WithWriteMutex(f func() error) error {
	r.writeMutex.Lock()
	defer r.writeMutex.Unlock()
	return f()
}

func (r RedisConnection) Read() (serde.Value, error) {
	return r.reader.Read()
}

func (r RedisConnection) CanRead() bool {
	return r.reader.CanRead()
}

func (r RedisConnection) ReadRDB() error {
	return r.reader.ReadRDB()
}

func (r RedisConnection) Close() {
	r.conn.Close()
}

func NewRedisConnection(c net.Conn) RedisConnection {
	reader := serde.NewReader(c)
	writer := serde.NewWriter(c)

	return RedisConnection{
		reader:           &reader,
		writer:           writer,
		conn:             c,
		readMutex:        &sync.Mutex{},
		writeMutex:       &sync.Mutex{},
		id:               uniuri.NewLen(40),
		transaction:      false,
		bufferedCommands: []serde.Value{},
	}
}
