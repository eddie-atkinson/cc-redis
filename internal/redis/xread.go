package redis

import (
	"codecrafters/internal/kvstore"
	"codecrafters/internal/serde"
	"context"
	"strconv"
	"time"
)

type xreadArgs struct {
	key string
	id  string
}

func parseXReadArgs(args []string) []xreadArgs {
	res := []xreadArgs{}
	if len(args)%2 != 0 {
		panic("Expected xread args to have key, id pairs")
	}
	for i := 0; i < len(args)/2; i = i + 1 {
		key := args[i]
		id := args[len(args)-1-i]

		res = append(res, xreadArgs{key, id})
	}
	return res
}

func xreadBlocking(ctx context.Context, r Redis, args []string) []serde.Value {
	blockMs, err := strconv.Atoi(args[0])

	if err != nil {
		return []serde.Value{serde.NewError("Expected blocking ms to be integer for xread")}
	}

	parsedArgs := parseXReadArgs(args[2:])
	resultChans := make([]chan kvstore.BlockingQueryResult, len(parsedArgs))

	for i, stream := range parsedArgs {
		resultChans[i] = make(chan kvstore.BlockingQueryResult)
		go r.store.ReadStreamBlocking(ctx, stream.key, stream.id, resultChans[i])
	}

	var timeoutChan <-chan time.Time

	if blockMs != 0 {
		timeoutChan = time.After(time.Duration(blockMs) * time.Millisecond)
	}

	for _, resultChan := range resultChans {
		select {
		case <-ctx.Done():
			continue
		case result := <-resultChan:
			streamArr := serde.NewArray([]serde.Value{serde.NewBulkString(result.Key), processXRangeOutput(result.Values)})
			return []serde.Value{serde.NewArray([]serde.Value{streamArr})}
		case <-timeoutChan:
			return []serde.Value{serde.NewNull()}
		}
	}
	return []serde.Value{serde.NewNull()}
}

func (r Redis) xread(ctx context.Context, args []string) []serde.Value {
	if len(args) < 3 {
		return []serde.Value{serde.NewError("XREAD expects at least three arguments")}
	}

	if args[0] == "block" {
		return xreadBlocking(ctx, r, args[1:])
	}

	parsedArgs := parseXReadArgs(args[1:])

	outputArr := []serde.Value{}

	for _, stream := range parsedArgs {
		queryResult, err := r.store.ReadStream(ctx, stream.key, stream.id)

		if err != nil {
			continue
		}

		streamRes := processXRangeOutput(queryResult)
		streamArr := serde.NewArray([]serde.Value{serde.NewBulkString(stream.key), streamRes})
		outputArr = append(outputArr, streamArr)
	}

	return []serde.Value{serde.NewArray(outputArr)}
}
