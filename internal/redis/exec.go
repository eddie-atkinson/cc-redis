package redis

import (
	"codecrafters/internal/serde"
	"context"
)

func (r Redis) exec(ctx context.Context, _ []string, connection RedisConnection) []serde.Value {
	if !connection.transaction {
		connection.WithWriteMutex(func() error {
			return connection.Send([]serde.Value{serde.NewError("ERR EXEC without MULTI")})
		})
	}
	result := []serde.Value{}
	for _, command := range connection.bufferedCommands {
		cmd, args, err := r.parseCommand(command)

		if err != nil {
			panic(err)
		}

		response, _ := r.executeAndMaybePropagate(ctx, cmd, args, command, connection)
		for _, v := range response {
			result = append(result, v)
		}
	}

	return []serde.Value{serde.NewArray(result)}
}
