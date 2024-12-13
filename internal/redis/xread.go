package redis

import (
	"codecrafters/internal/serde"
	"context"
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

func (r Redis) xread(ctx context.Context, args []string) []serde.Value {
	if len(args) < 3 {
		return []serde.Value{serde.NewError("XREAD expects at least three arguments")}
	}

	if args[0] != "streams" {
		panic("Only currently supports streams")
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
