package kvstore

import (
	"codecrafters/internal/serde"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/armon/go-radix"
)

type StreamId struct {
	timestamp uint64
	seqNo     uint64
}

const (
	STREAM_ID_DELIMETER = "-"
)

func ParseStreamId(input string) (StreamId, error) {
	parts := strings.Split(input, STREAM_ID_DELIMETER)
	if len(parts) != 2 {
		return StreamId{}, fmt.Errorf("expected two parts in stream ID got %d", len(parts))
	}
	msTime, err := strconv.Atoi(parts[0])

	if err != nil {
		return StreamId{}, err
	}
	seqNo, err := strconv.Atoi(parts[1])

	if err != nil {
		return StreamId{}, err
	}
	if seqNo < 1 {
		return StreamId{}, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if msTime < 0 {
		return StreamId{}, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	return StreamId{timestamp: uint64(msTime), seqNo: uint64(seqNo)}, nil

}

func (id StreamId) ToString() string {
	return fmt.Sprintf("%d-%d", id.timestamp, id.seqNo)
}

type StoredStream struct {
	value radix.Tree
}

func (ss StoredStream) validateInsertionKey(key StreamId) error {
	lastKey, _, exists := ss.value.Maximum()

	if !exists {
		return nil
	}

	lastId, err := ParseStreamId(lastKey)

	if err != nil {
		return err
	}

	fmt.Printf("lastKey %v, newKey: %v", lastId, key)

	if lastId.timestamp > key.timestamp {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	if lastId.timestamp == key.timestamp && lastId.seqNo >= key.seqNo {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

func NewStoredStream() StoredStream {
	return StoredStream{value: *radix.New()}
}

func (ss StoredStream) Type() string {
	return "stream"
}

func (ss StoredStream) Insert(id StreamId, value map[string]string) error {
	err := ss.validateInsertionKey(id)

	if err != nil {
		return err
	}

	ss.value.Insert(id.ToString(), value)
	return nil
}

func (ss StoredStream) Value() serde.Value {
	panic("No idea how to serialise this yet")
}

func (ss StoredStream) IsExpired(ctx context.Context) bool {
	// TODO (eatkinson): Maybe implement this?
	return false
}
