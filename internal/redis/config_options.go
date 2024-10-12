package redis

import (
	"errors"
	"flag"
	"strconv"
	"strings"

	"github.com/dchest/uniuri"
)

const DEFAULT_PERSISTENCE_FILE_NAME string = "dump.rdb"
const DEFAULT_PERSISTENCE_DIR string = "./"
const DEFAULT_PORT = 6379

const (
	SLAVE  = 0
	MASTER = 1
)

type replicaConfig interface {
	Role() string
}

type slaveConfig struct {
	host string
	port int
}

func (s slaveConfig) Role() string {
	return "slave"
}

type masterConfig struct{}

func (m masterConfig) Role() string {
	return "master"
}

type replicationConfig struct {
	replicaConfig    replicaConfig
	masterReplId     string
	masterReplOffset int
}

func parseReplicaString(replicaString string) (replicaConfig, error) {
	defaultErr := masterConfig{}

	if len(replicaString) == 0 {
		return masterConfig{}, nil
	}

	split := strings.Split(replicaString, " ")
	if len(split) != 2 {
		return defaultErr, errors.New("expected replica string to be of form: '<host> <port>'")
	}

	host := split[0]
	port, err := strconv.Atoi(split[1])

	if err != nil {
		return defaultErr, err
	}
	return slaveConfig{
		host,
		port,
	}, nil
}

func newReplicationConfig(replicaOf string) (replicationConfig, error) {

	config := replicationConfig{
		masterReplId:     uniuri.NewLen(40),
		masterReplOffset: 0,
	}

	replicaConfig, err := parseReplicaString(replicaOf)

	if err != nil {
		return config, err
	}

	config.replicaConfig = replicaConfig

	return config, nil
}

type configurationOptions struct {
	persistenceFileName string
	persistenceDir      string
	port                int
	replicationConfig   replicationConfig
}

func ParseConfigurationFromFlags() (configurationOptions, error) {
	opts := configurationOptions{}

	replicaOf := ""

	flag.StringVar(&opts.persistenceFileName, "dbfilename", DEFAULT_PERSISTENCE_FILE_NAME, "File name to store persisted data in")
	flag.StringVar(&opts.persistenceDir, "dir", DEFAULT_PERSISTENCE_DIR, "Directory to store the persisted data in")
	flag.IntVar(&opts.port, "port", DEFAULT_PORT, "Port to listen on for connections")
	flag.StringVar(&replicaOf, "replicaof", "", "Host and port to replicate from")
	flag.Parse()

	replicationConfig, err := newReplicationConfig(replicaOf)

	if err != nil {
		return opts, err
	}

	opts.replicationConfig = replicationConfig
	return opts, nil
}
