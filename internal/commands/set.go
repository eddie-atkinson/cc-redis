package commands

import (
	"codecrafters/internal/serde"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type setArgs struct {
	expiresAt uint64
}

type storedValue struct {
	value     string
	expiresAt uint64
}

func nowMilli() uint64 {
	return uint64((time.Now().UnixNano() / int64(time.Millisecond)))
}

func parseSetArgs(args []string) (setArgs, error) {
	// Retain the key until the heat death of the universe by default
	parsedArgs := setArgs{math.MaxUint64}

	// TODO (eatkinson): Support more arguments
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
				parsedArgs.expiresAt = nowMilli() + uint64(expiry)
				i++
			}
		}
	}
	return parsedArgs, nil
}

var store = newKVStore()

func set(args []string) serde.Value {
	if len(args) < 2 {
		return serde.NewError("SET expects at least two arguments")
	}

	key := args[0]
	value := args[1]

	parsedArgs, err := parseSetArgs(args[2:])

	if err != nil {
		return serde.NewError(err.Error())
	}

	store.setKey(key, storedValue{value: value, expiresAt: parsedArgs.expiresAt})

	return serde.NewSimpleString("OK")
}
