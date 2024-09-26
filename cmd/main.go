package main

import (
	"fmt"
	"net"
	"os"

	"codecrafters/internal/redis"
)

func main() {
	port := ":6379"

	fmt.Println("Listening on ", port)

	l, err := net.Listen("tcp", port)

	redis := redis.NewRedis()

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
