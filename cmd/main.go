package main

import (
	"fmt"
	"os"

	"codecrafters/internal/redis"
)

func main() {

	redis, err := redis.NewRedisWithConfig()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = redis.Init()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
