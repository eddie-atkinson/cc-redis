package kvstore

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type StreamId struct {
	timestamp uint64
	seqNo     uint64
}

const (
	STREAM_ID_DELIMETER = "-"
	WILDCARD            = "*"
	START_OF_STREAM     = "-"
	END_OF_STREAM       = "+"
)

func GetQueryStreamId(id string) (StreamId, error) {
	if id == START_OF_STREAM {
		return StreamId{0, 0}, nil
	}
	if id == END_OF_STREAM {
		return StreamId{math.MaxUint64, math.MaxUint64}, nil
	}
	parts := strings.Split(id, STREAM_ID_DELIMETER)
	msTime, err := strconv.Atoi(parts[0])

	if err != nil {
		return StreamId{}, err
	}

	if len(parts) == 1 {
		var seqNo uint64 = 0
		if msTime == 0 {
			seqNo += 1
		}
		return StreamId{uint64(msTime), seqNo}, nil
	}

	seqNo, err := strconv.Atoi(parts[1])

	if err != nil {
		return StreamId{}, err
	}

	return StreamId{timestamp: uint64(msTime), seqNo: uint64(seqNo)}, nil

}

func parseStreamId(input string) (StreamId, error) {
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

	if seqNo < 0 {
		return StreamId{}, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if msTime < 0 {
		return StreamId{}, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if msTime == 0 && seqNo == 0 {
		return StreamId{}, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	return StreamId{timestamp: uint64(msTime), seqNo: uint64(seqNo)}, nil

}

func (id StreamId) ToString() string {
	return fmt.Sprintf("%d-%d", id.timestamp, id.seqNo)
}
