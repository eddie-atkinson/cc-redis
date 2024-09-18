package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"codecrafters/internal/commands"
	"codecrafters/internal/serde"
)

func handleConnection(c net.Conn) {
	defer c.Close()
	for {
		resp := serde.NewResp(c)
		writer := serde.NewWriter(c)

		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from the client: ", err.Error())
			return
		}

		switch v := value.(type) {
		case serde.Array:
			commandArray, err := v.ToCommandArray()
			if err != nil {
				writer.Write(serde.NewError(err.Error()))
			}

			writer.Write(commands.ExecuteCommand(commandArray))
		default:
			writer.Write(serde.NewError("Expected commands to be array"))
		}
	}

}

func main() {
	port := ":6379"

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
		go handleConnection(conn)
	}

}
