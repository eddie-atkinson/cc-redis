package redis

import (
	"fmt"
)

func initMaster(r *Redis) error {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go r.handleConnection(conn)
	}
}
