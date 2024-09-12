package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

type Offset struct {
	start int
	end   int
	word  []byte
}

type RedisType int

const (
	RedisSimpleString RedisType = iota
	RedisError
)

type RedisValue interface {
	Type() RedisType
	String() string
	Error() string
}

type SimpleString struct {
	data []byte
}

func (s SimpleString) Type() RedisType {
	return RedisSimpleString
}

func (s SimpleString) String() string {
	return string(s.data[:])
}

func (s SimpleString) Error() (string, error) {
	return "", InvalidTypeCoercion{}
}

func word(buf []byte, offset int) (Offset, error) {
	currentOffset := offset
	bufLen := len(buf)

	if bufLen < offset {
		return Offset{offset, offset, nil}, EndOfBuffer{}
	}

	for currentOffset < bufLen && buf[currentOffset] != '\r' {
		currentOffset++
	}

	if currentOffset+1 < bufLen {
		return Offset{offset, currentOffset + 2, buf[offset:currentOffset]}, nil
	}

	return Offset{offset, offset, nil}, EndOfBuffer{}
}

func parse(buf []byte) RedisValue {
	// TODO: parse the body of the buffer into a RedisValue depending on its magic byte
	panic("TODO")
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		b := make([]byte, 1024)

		d, err := conn.Read(b)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Printf("Received %b\n", d)
		fmt.Printf("Byte width: %d\n", len(b))
		conn.Write([]byte("+PONG\r\n"))
	}

}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// Close the connection eventually
	defer l.Close()

	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConnection(conn)
	}

}
