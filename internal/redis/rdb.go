package redis

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	mapset "github.com/deckarep/golang-set/v2"
)

const (
	REDIS_ASCII_BYTES      = "REDIS"
	MAGIC_BYTE_HEADER_SIZE = 9
	EOF                    = 0xFF
	SELECT_DB              = 0xFE
	EXPIRE_TIME            = 0xFD
	EXPIRE_TIME_MS         = 0xFC
	RESIZE_DB              = 0xFB
	NULL_BYTE              = 0x00
	HASH_TABLE_SIZE        = 0xFB
	EXPIRY_SECONDS         = 0xFD
	EXPIRY_MS              = 0xFC

	// Value types for Redis encoding: https://rdb.fnordig.de/file_format.html#string-encoding
	// We only support string because I'm lazy
	STRING_VALUE = 0x00
)

// Variable integer encoding consts
const (
	SIX_BIT_INT      = 0
	FOURTEEN_BIT_INT = 1
	FOUR_BYTE_INT    = 2
	STRING_ENCODED   = 3
)

// String length encoding consts
const (
	ONE_BYTE_STRING_SIZE  = 0
	TWO_BYTE_STRING_SIZE  = 1
	FOUR_BYTE_STRING_SIZE = 2
)

const (
	EmptyDBError EmptyDB = "DB section is empty"
)

type sizeEncoded interface {
	Size() int
}

type integerSizeEncoded struct {
	size int
}

type stringSizeEncoded struct {
	size int
}

func (i integerSizeEncoded) Size() int {
	return i.size
}

func (s stringSizeEncoded) Size() int {
	return s.size
}

type persistedDB struct {
	values []keyValuePair
	id     int
}

type keyValuePair struct {
	key        string
	value      string
	expiryInMs *uint64
}

type EmptyDB string

func (e EmptyDB) Error() string {
	return string(e)
}

func parseStringEncoded(reader *bufio.Reader, stringType int) (*stringSizeEncoded, error) {

	switch stringType {
	case ONE_BYTE_STRING_SIZE:
		nextByte, err := reader.ReadByte()

		if err != nil {
			return nil, err
		}
		return &stringSizeEncoded{int(nextByte)}, nil

	case TWO_BYTE_STRING_SIZE:
		stringSize, err := readNBytes(reader, 2)

		if err != nil {
			return nil, err
		}
		length := int(binary.LittleEndian.Uint16(stringSize))

		return &stringSizeEncoded{int(length)}, nil
	case FOUR_BYTE_STRING_SIZE:
		stringSize, err := readNBytes(reader, 4)
		if err != nil {
			return nil, err
		}
		length := int(binary.LittleEndian.Uint32(stringSize))
		return &stringSizeEncoded{int(length)}, nil
	default:
		return nil, errors.New("failed to parse string encoded bytes")
	}

}
func parseSizeEncodedInteger(reader *bufio.Reader) (sizeEncoded, error) {

	firstByte, err := reader.ReadByte()
	defaultErr := integerSizeEncoded{-1}

	if err != nil {
		return defaultErr, err
	}

	controlBits := firstByte >> 6

	switch controlBits {
	case SIX_BIT_INT:
		return integerSizeEncoded{size: int(firstByte & 0b00111111)}, nil
	case FOURTEEN_BIT_INT:
		nextByte, err := reader.ReadByte()

		if err != nil {
			return defaultErr, err
		}
		prefixBits := firstByte & 0b00111111
		byteArray := []byte{prefixBits, nextByte}
		value := binary.BigEndian.Uint16(byteArray)
		return integerSizeEncoded{int(value)}, nil
	case FOUR_BYTE_INT:
		nextFourBytes, err := readNBytes(reader, 4)

		if err != nil {
			return defaultErr, err
		}

		value := binary.BigEndian.Uint32(nextFourBytes)
		return integerSizeEncoded{int(value)}, nil

	case STRING_ENCODED:
		stringType := int(firstByte & 0b00111111)
		return parseStringEncoded(reader, stringType)

	default:
		return defaultErr, errors.New("encountered unexpected byte sequence when parsing size encoded value")
	}

}

func parseHeader(reader *bufio.Reader) error {
	magicString := make([]byte, MAGIC_BYTE_HEADER_SIZE)
	readBytes, err := reader.Read(magicString)

	if err != nil {
		return err
	}

	if readBytes != MAGIC_BYTE_HEADER_SIZE {
		return fmt.Errorf("expected to read %d bytes for RDB header, instead got %d", MAGIC_BYTE_HEADER_SIZE, readBytes)
	}

	redisAscii := []byte(REDIS_ASCII_BYTES)

	hasRedisPrefix := bytes.Equal(redisAscii, magicString[:5])
	_, err = strconv.Atoi(string(magicString[5:]))

	magicByteErrorMessage := "Expected RDB file to start with magic byte REDIS followed by 4 digits"

	if err != nil {
		return errors.New(magicByteErrorMessage)
	}

	if !hasRedisPrefix {
		return errors.New(magicByteErrorMessage)
	}

	return nil
}

func readUntilOneOf(reader *bufio.Reader, delimiters mapset.Set[byte]) ([]byte, byte, error) {
	full := make([]byte, 1024)

	for {

		b, err := reader.ReadByte()

		if err != nil {
			return full, NULL_BYTE, err
		}

		full = append(full, b)

		if delimiters.Contains(b) {
			return full, b, nil
		}
	}

}

func readNBytes(reader *bufio.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)

	_, err := io.ReadFull(reader, buf)

	return buf, err
}

// We should probably read / care about this section. But the metadata is not going to be used for our later
// parsing of this file. So instead we just read until we hit the DB section of the file
func parseMetadataSection(reader *bufio.Reader) (byte, error) {
	_, nextSection, err := readUntilOneOf(reader, mapset.NewSet[byte](SELECT_DB, EOF))
	return nextSection, err
}

func parseDBKey(reader *bufio.Reader, valueType byte, expiresAt *uint64) (*keyValuePair, error) {
	if valueType != STRING_VALUE {
		return nil, errors.New("does not support values not encoded as strings")
	}

	keyLen, err := parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}

	key, err := readNBytes(reader, keyLen.Size())

	if err != nil {
		return nil, err
	}

	valueLen, err := parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}
	value, err := readNBytes(reader, valueLen.Size())

	if err != nil {
		return nil, err
	}

	return &keyValuePair{key: string(key), value: string(value), expiryInMs: expiresAt}, nil
}

func parseDBKeys(reader *bufio.Reader, count int) ([]keyValuePair, error) {
	values := []keyValuePair{}

	for range count {
		firstByte, err := reader.ReadByte()
		if err != nil {
			return values, err
		}
		switch firstByte {
		case EXPIRY_SECONDS:
			expireInSeconds, err := readNBytes(reader, 4)

			if err != nil {
				return values, err
			}

			expiryInMs := uint64(binary.LittleEndian.Uint32(expireInSeconds)) * 1000

			valueFlag, err := reader.ReadByte()

			if err != nil {
				return values, err
			}

			entry, err := parseDBKey(reader, valueFlag, &expiryInMs)

			if err != nil {
				return values, err
			}

			values = append(values, *entry)

		case EXPIRE_TIME_MS:
			expiryInMs, err := readNBytes(reader, 8)

			if err != nil {
				return values, err
			}

			expiry := binary.LittleEndian.Uint64(expiryInMs)

			valueFlag, err := reader.ReadByte()

			if err != nil {
				return values, err
			}

			entry, err := parseDBKey(reader, valueFlag, &expiry)

			if err != nil {
				return values, err
			}

			values = append(values, *entry)

		case STRING_VALUE:
			entry, err := parseDBKey(reader, firstByte, nil)

			if err != nil {
				return values, err
			}

			values = append(values, *entry)
		default:
			return values, errors.New("encountered unexpected DB Key magic byte")
		}
	}

	return values, nil
}

func parseDBEntry(reader *bufio.Reader) (*persistedDB, error) {
	databaseId, err := parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}

	// Double check that this DB actually contains keys before going any further
	hashTableMagicByte, err := reader.Peek(1)

	if err != nil {
		return nil, err
	}

	if len(hashTableMagicByte) != 1 {
		return nil, errors.New("failed attempting to read magic hash table byte")
	}

	if hashTableMagicByte[0] != HASH_TABLE_SIZE {
		return nil, EmptyDBError
	}

	_, err = reader.ReadByte()

	if err != nil {
		return nil, err
	}

	hashTableSize, err := parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}

	// I should care about the number of keys with expiries, but I really don't
	_, err = parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}

	values, err := parseDBKeys(reader, hashTableSize.Size())

	if err != nil {
		return nil, err
	}

	return &persistedDB{
		values: values,
		id:     databaseId.Size(),
	}, nil

}

func parseDBSection(reader *bufio.Reader) (byte, []persistedDB, error) {

	persisted := []persistedDB{}

	parsedDB, err := parseDBEntry(reader)

	if errors.Is(err, EmptyDBError) {
		nextSection, err := reader.ReadByte()
		return nextSection, persisted, err
	}

	if err != nil {
		return NULL_BYTE, persisted, err
	}

	persisted = append(persisted, *parsedDB)

	nextSection, err := reader.ReadByte()

	if err != nil {
		return nextSection, persisted, err
	}

	for nextSection == SELECT_DB {
		parsedDB, err = parseDBEntry(reader)

		if err != nil {
			return NULL_BYTE, persisted, err
		}

		if persisted != nil {
			persisted = append(persisted, *parsedDB)
		}

		nextSection, err := reader.ReadByte()

		if err != nil {
			return nextSection, persisted, err
		}
	}

	return nextSection, persisted, nil
}

func (r Redis) processRDBFile() error {
	persistencePath := path.Join(r.configuration.persistenceDir, r.configuration.persistenceFileName)
	file, err := os.Open(persistencePath)

	// If there's nothing to read, there's no more for us to do
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	err = parseHeader(reader)

	if err != nil {
		return err
	}
	nextSection, err := parseMetadataSection(reader)

	if err != nil {
		return err
	}

	if nextSection == SELECT_DB {
		nextSection, persistedDBs, err := parseDBSection(reader)

		if err != nil {
			return err
		}
		for _, db := range persistedDBs {
			for _, kvPair := range db.values {
				r.store.SetKeyWithExpiresAt(kvPair.key, kvPair.value, kvPair.expiryInMs)
			}
		}

		if nextSection == EOF {
			return nil
		}
	}

	if nextSection == EOF {
		return nil
	}

	readByte, err := reader.ReadBytes(EOF)

	if readByte[len(readByte)-1] == EOF {
		return nil
	}
	// Yes there's a CRC, I'm tired
	return err

}
