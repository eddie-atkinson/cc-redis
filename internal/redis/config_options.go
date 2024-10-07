package redis

import (
	"errors"
	"flag"
	"strconv"
	"strings"
)

const DEFAULT_PERSISTENCE_FILE_NAME string = "dump.rdb"
const DEFAULT_PERSISTENCE_DIR string = "./"
const DEFAULT_PORT = 6379

const (
	SLAVE  = 0
	MASTER = 1
)

type replicaConfig struct {
	host string
	port int
}

func parseReplicaString(replicaString string) (*replicaConfig, error) {
	if len(replicaString) == 0 {
		return nil, nil
	}
	split := strings.Split(replicaString, " ")
	if len(split) != 2 {
		return nil, errors.New("expected replica string to be of form: '<host> <port>'")
	}

	host := split[0]
	port, err := strconv.Atoi(split[1])

	if err != nil {
		return nil, err
	}
	return &replicaConfig{
		host,
		port,
	}, nil
}

type configurationOptions struct {
	persistenceFileName string
	persistenceDir      string
	port                int
	replicaConfig       *replicaConfig
}

func ParseConfigurationFromFlags() (configurationOptions, error) {
	opts := configurationOptions{}

	replicaOf := ""

	flag.StringVar(&opts.persistenceFileName, "dbfilename", DEFAULT_PERSISTENCE_FILE_NAME, "File name to store persisted data in")
	flag.StringVar(&opts.persistenceDir, "dir", DEFAULT_PERSISTENCE_DIR, "Directory to store the persisted data in")
	flag.IntVar(&opts.port, "port", DEFAULT_PORT, "Port to listen on for connections")
	flag.StringVar(&replicaOf, "replicaof", "", "Host and port to replicate from")
	flag.Parse()

	replicaConfig, err := parseReplicaString(replicaOf)

	opts.replicaConfig = replicaConfig

	if err != nil {
		return opts, err
	}

	return opts, nil
}
