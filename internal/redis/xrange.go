package redis

import (
	"codecrafters/internal/array"
	"codecrafters/internal/serde"
	"context"
)

func (r Redis) xrange(ctx context.Context, args []string) []serde.Value {
	if len(args) < 3 {
		return []serde.Value{serde.NewError("XRANGE expects at least three arguments")}
	}

	key := args[0]
	startId := args[1]
	endId := args[2]

	queryResult, err := r.store.QueryStream(ctx, key, startId, endId)

	if err != nil {
		return []serde.Value{serde.NewError(err.Error())}
	}

	outputArr := []serde.Value{}

	for _, res := range queryResult {
		kvArr := []serde.Value{serde.NewBulkString(res.Id)}
		serialisedKV := array.Map(res.Values, func(s string) serde.Value {
			return serde.NewBulkString(s)
		})

		kvArr = append(kvArr, serde.NewArray(serialisedKV))
		outputArr = append(outputArr, serde.NewArray(kvArr))
	}

	return []serde.Value{serde.NewArray(outputArr)}
}
