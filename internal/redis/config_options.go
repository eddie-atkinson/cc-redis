package redis

import "flag"

const DEFAULT_PERSISTENCE_FILE_NAME string = "dump.rdb"
const DEFAULT_PERSISTENCE_DIR string = "./"

type configurationOptions struct {
	persistenceFileName string
	persistenceDir      string
}

func ParseConfigurationFromFlags() configurationOptions {
	opts := configurationOptions{}

	flag.StringVar(&opts.persistenceFileName, "dbfilename", DEFAULT_PERSISTENCE_FILE_NAME, "File name to store persisted data in")
	flag.StringVar(&opts.persistenceDir, "dir", DEFAULT_PERSISTENCE_DIR, "Directory to store the persisted data in")
	flag.Parse()

	return opts
}
