package serde

import (
	"reflect"
	"testing"
)

func TestArray_Marshal(t *testing.T) {
	type fields struct {
		items []Value
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			"Should marshal an empty array correctly",
			fields{[]Value{}},
			[]byte("*0\r\n"),
		},
		{
			"Should marshal an array containing a bulk string correctly",
			fields{[]Value{
				NewBulkString(
					"hello",
				),
				NewBulkString(
					"world",
				),
			}},
			[]byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := Array{
				Items: tt.fields.items,
			}
			if got := bs.Marshal(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Array.Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
