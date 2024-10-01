package redis

import (
	"bufio"
	"os"
	"reflect"
	"testing"
)

func createAndWriteTempFile(name string, body []byte) (*os.File, error) {
	f, err := os.CreateTemp("", name)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(body)

	if err != nil {
		return nil, err
	}
	f.Seek(0, 0)

	return f, nil

}
func Test_parseSizeEncoded(t *testing.T) {

	tests := []struct {
		name     string
		fileData []byte
		want     sizeEncoded
		wantErr  bool
	}{
		{
			name:     "It should parse a single byte integer correctly",
			fileData: []byte{0b00000101},
			want:     integerSizeEncoded{5},
			wantErr:  false,
		},
		{
			name: "It should parse a 14 bit integer correctly",
			fileData: []byte{
				0b01011111,
				0b01011010,
			},
			want:    integerSizeEncoded{8026},
			wantErr: false,
		},
		{
			name: "It should parse a 4 byte integer correctly",
			fileData: []byte{
				0b10111111,
				0b00000000,
				0b00000100,
				0b00000000,
				0b00000001,
			},
			want:    integerSizeEncoded{262_145},
			wantErr: false,
		},
		{
			name: "It should parse a string with an 8 bit length value correctly",
			fileData: []byte{
				0xC0, // String with 8 bit length to come
				0x0D, // Length is 13 chars
				0x48, // H
				0x65, // e
				0x6C, // l
				0x6C, // l
				0x6F, // o
				0x2C, // ,
				0x20, // " "
				0x57, // W
				0x6F, // o
				0x72, // r
				0x6C, // l
				0x64, // d
				0x21, // !
			},
			want:    stringSizeEncoded{13, "Hello, World!"},
			wantErr: false,
		},
		{
			name: "It should parse a string with an 16 bit length value correctly",
			fileData: []byte{
				0xC1, // String with 16 bit length to come
				0x0D, // Length is 13 chars
				0x00,
				0x48, // H
				0x65, // e
				0x6C, // l
				0x6C, // l
				0x6F, // o
				0x2C, // ,
				0x20, // " "
				0x57, // W
				0x6F, // o
				0x72, // r
				0x6C, // l
				0x64, // d
				0x21, // !
			},
			want:    stringSizeEncoded{13, "Hello, World!"},
			wantErr: false,
		},
		{
			name: "It should parse a string with an 32 bit length value correctly",
			fileData: []byte{
				0xC2, // String with 32 bit length to come
				0x0D, // Length is 13 chars
				0x00,
				0x00,
				0x00,
				0x48, // H
				0x65, // e
				0x6C, // l
				0x6C, // l
				0x6F, // o
				0x2C, // ,
				0x20, // " "
				0x57, // W
				0x6F, // o
				0x72, // r
				0x6C, // l
				0x64, // d
				0x21, // !
			},
			want:    stringSizeEncoded{13, "Hello, World!"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := createAndWriteTempFile(tt.name, tt.fileData)

			if err != nil {
				t.Fail()
			}

			defer f.Close()
			defer os.Remove(f.Name())

			got, err := parseSizeEncodedInteger(bufio.NewReader(f))
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSizeEncoded() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSizeEncoded() = %v, want %v", got, tt.want)
			}
		})
	}
}
