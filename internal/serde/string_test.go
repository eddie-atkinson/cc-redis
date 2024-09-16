package serde

import (
	"reflect"
	"testing"
)

func TestBulkString_Marshal(t *testing.T) {
	type fields struct {
		value string
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			"Should marhsal a bulk string correctly",
			fields{"ECHO FOO"},
			[]byte("$8\r\nECHO FOO\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := BulkString{
				value: tt.fields.value,
			}
			if got := bs.Marshal(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BulkString.Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimpleString_Marshal(t *testing.T) {
	type fields struct {
		value string
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			"Should marhsal a simple string correctly",
			fields{"ECHO FOO"},
			[]byte("+ECHO FOO\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := SimpleString{
				Value: tt.fields.value,
			}
			if got := ss.Marshal(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SimpleString.Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
