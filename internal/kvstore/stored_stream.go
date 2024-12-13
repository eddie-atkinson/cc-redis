package kvstore

import (
	"codecrafters/internal/serde"
	"codecrafters/internal/time"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/armon/go-radix"
	"github.com/tilinna/clock"
)

type StoredStream struct {
	value radix.Tree
}

type StreamQueryResult struct {
	Id     string
	Values []string
}

func (ss StoredStream) generateStreamId(ctx context.Context, input string) (StreamId, error) {

	now := time.NowMilli(clock.FromContext(ctx))
	newId := StreamId{timestamp: now, seqNo: 0}
	if input == WILDCARD {
		return newId, nil
	}

	parts := strings.Split(input, STREAM_ID_DELIMETER)
	if len(parts) != 2 {
		return StreamId{}, fmt.Errorf("expected two parts in stream ID got %d", len(parts))
	}

	if parts[1] == WILDCARD {

		var seqNoCount uint64 = 0

		msTime, err := strconv.Atoi(parts[0])

		if err != nil {
			return StreamId{}, err
		}

		ss.value.WalkPrefix(parts[0], func(s string, v interface{}) bool {
			fmt.Printf("%v %v", input, s)
			seqNoCount += 1
			return false
		})

		if msTime == 0 && seqNoCount == 0 {
			// Redis only permits IDs that start counting from 0-1
			seqNoCount += 1
		}
		return StreamId{timestamp: uint64(msTime), seqNo: seqNoCount}, nil
	}
	id, err := parseStreamId(input)

	if err != nil {
		return StreamId{}, err
	}

	err = ss.validateInsertionKey(id)
	return id, err
}
func (ss StoredStream) validateInsertionKey(key StreamId) error {
	lastKey, _, exists := ss.value.Maximum()

	if !exists {
		return nil
	}

	lastId, err := parseStreamId(lastKey)

	if err != nil {
		return err
	}

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

func (ss StoredStream) Insert(ctx context.Context, id string, value map[string]string) (StreamId, error) {
	streamId, err := ss.generateStreamId(ctx, id)

	if err != nil {
		return streamId, err
	}

	ss.value.Insert(streamId.ToString(), value)
	return streamId, nil
}

func (ss StoredStream) Value() serde.Value {
	panic("No idea how to serialise this yet")
}

func (ss StoredStream) IsExpired(ctx context.Context) bool {
	// TODO (eatkinson): Maybe implement this?
	return false
}
