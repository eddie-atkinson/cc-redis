package main

import (
	"fmt"
	"net"
	"os"

	"codecrafters/internal/redis"
)

func main() {

	redis := redis.NewRedisWithConfig()
	err := redis.Init()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	port := fmt.Sprintf(":%d", redis.Port())

	fmt.Println("Listening on ", port)

	l, err := net.Listen("tcp", port)

	if err != nil {
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go redis.HandleConnection(conn)
	}

}
