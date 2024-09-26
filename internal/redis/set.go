package redis

import (
	"codecrafters/internal/serde"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type setArgs struct {
	expireInMs *uint64
}

func parseSetArgs(args []string) (setArgs, error) {
	parsedArgs := setArgs{expireInMs: nil}

	// TODO (eatkinson): Support more arguments and clean up the parsing here
	for i := 0; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "px":
			{
				if i+1 == len(args) {
					return parsedArgs, errors.New("PX must have expiry time")
				}
				expiry, err := strconv.Atoi(args[i+1])
				if err != nil || expiry < 0 {
					return parsedArgs, fmt.Errorf("expiry must be a positive integer")
				}
				expiryPtr := new(uint64)
				*expiryPtr = uint64(expiry)

				parsedArgs.expireInMs = expiryPtr
				i++
			}
		}
	}
	return parsedArgs, nil
}

func (r Redis) set(ctx context.Context, args []string) serde.Value {
	if len(args) < 2 {
		return serde.NewError("SET expects at least two arguments")
	}

	key := args[0]
	value := args[1]

	parsedArgs, err := parseSetArgs(args[2:])

	if err != nil {
		return serde.NewError(err.Error())
	}

	r.store.SetKeyWithExpiry(ctx, key, value, parsedArgs.expireInMs)

	return serde.NewSimpleString("OK")
}
