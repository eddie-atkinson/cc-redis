package redis

import (
	"codecrafters/internal/serde"
	"context"
	"fmt"
	"io"
	"net"
)

func (r Redis) handleConnection(c net.Conn) {
	defer c.Close()
	for {
		reader := serde.NewReader(c)
		writer := serde.NewWriter(c)
		ctx := context.Background()

		value, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from the client: ", err.Error())
			return
		}
		writer.Write(r.executeCommand(ctx, value))
	}
}

func initMaster(r Redis) error {
	port := fmt.Sprintf(":%d", r.Port())
	fmt.Println("Listening on ", port)
	l, err := net.Listen("tcp", port)

	if err != nil {
		return err
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go r.handleConnection(conn)
	}
}
