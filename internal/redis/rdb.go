package redis

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

type sizeEncoded interface {
	Size() int
}

type integerSizeEncoded struct {
	size int
}

type stringSizeEncoded struct {
	size  int
	value string
}

func (i integerSizeEncoded) Size() int {
	return i.size
}

func (s stringSizeEncoded) Size() int {
	return s.size
}

func (s stringSizeEncoded) Value() string {
	return s.value
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

func readSizeEncodedString(reader *bufio.Reader, length int) (*stringSizeEncoded, error) {
	buf, err := readNBytes(reader, length)

	if err != nil {
		return nil, err
	}

	return &stringSizeEncoded{size: length, value: string(buf)}, nil
}

func parseStringEncoded(reader *bufio.Reader) (*stringSizeEncoded, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	controlBits := firstByte >> 6

	if controlBits != STRING_ENCODED {
		return nil, errors.New("expected string encoded value to have string control bits set")
	}

	stringType := firstByte & 0b00111111

	switch stringType {
	case ONE_BYTE_STRING_SIZE:
		nextByte, err := reader.ReadByte()

		if err != nil {
			return nil, err
		}
		return readSizeEncodedString(reader, int(nextByte))

	case TWO_BYTE_STRING_SIZE:
		stringSize, err := readNBytes(reader, 2)

		if err != nil {
			return nil, err
		}
		length := int(binary.LittleEndian.Uint16(stringSize))

		return readSizeEncodedString(reader, length)
	case FOUR_BYTE_STRING_SIZE:
		stringSize, err := readNBytes(reader, 4)
		if err != nil {
			return nil, err
		}
		return readSizeEncodedString(reader, int(binary.LittleEndian.Uint32(stringSize)))
	default:
		return nil, errors.New("")
	}

}
func parseSizeEncodedInteger(reader *bufio.Reader) (integerSizeEncoded, error) {

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
	read := 0

	for read < n {
		bytesRead, err := reader.Read(buf[read:])
		if err != nil {
			return buf[:read], fmt.Errorf("error reading from reader: %w", err)
		}
		read += bytesRead
	}

	return buf, nil
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
	theByte, _ := reader.ReadByte()
	fmt.Printf("%v", theByte)

	key, err := parseStringEncoded(reader)

	if err != nil {
		return nil, err
	}

	value, err := parseStringEncoded(reader)

	if err != nil {
		return nil, err
	}

	return &keyValuePair{key: key.Value(), value: value.Value(), expiryInMs: expiresAt}, nil
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
			expireInSeconds, err := parseSizeEncodedInteger(reader)
			if err != nil {
				return values, err
			}

			expiryInMs := uint64(expireInSeconds.Size() * 1000)

			valueFlag, err := reader.ReadByte()

			if err != nil {
				return values, nil
			}

			entry, err := parseDBKey(reader, valueFlag, &expiryInMs)

			if err != nil {
				return values, nil
			}

			values = append(values, *entry)

		case EXPIRE_TIME_MS:
			expiryInMs, err := parseSizeEncodedInteger(reader)

			if err != nil {
				return values, err
			}

			valueFlag, err := reader.ReadByte()

			if err != nil {
				return values, nil
			}

			expiryTimeInMs := uint64(expiryInMs.Size())

			entry, err := parseDBKey(reader, valueFlag, &expiryTimeInMs)

			if err != nil {
				return values, nil
			}

			values = append(values, *entry)

		case STRING_VALUE:
			entry, err := parseDBKey(reader, firstByte, nil)

			if err != nil {
				return values, nil
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

	hashTableMagicByte, err := reader.ReadByte()

	if err != nil {
		return nil, err
	}

	if hashTableMagicByte != HASH_TABLE_SIZE {
		return nil, errors.New("expected hash table magic byte to follow table ID")
	}

	hashTableSize, err := parseSizeEncodedInteger(reader)

	if err != nil {
		return nil, err
	}

	// I should care about the number of keys with expiries, but I really don't
	keysWithExpiry, err := parseSizeEncodedInteger(reader)

	fmt.Printf("Keys with expiry, %v\n", keysWithExpiry)
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
		persisted = append(persisted, *parsedDB)

		nextSection, err := reader.ReadByte()

		if err != nil {
			return nextSection, persisted, err
		}
	}

	return nextSection, persisted, nil
}

func (r Redis) processRDBFile(ctx context.Context) error {
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
				r.store.SetKeyWithExpiry(ctx, kvPair.key, kvPair.value, kvPair.expiryInMs)
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
