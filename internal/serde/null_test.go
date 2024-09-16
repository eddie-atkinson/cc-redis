package serde

import (
	"reflect"
	"testing"
)

func TestNull_Marshal(t *testing.T) {
	tests := []struct {
		name string
		null Null
		want []byte
	}{
		{
			"Should marhsal a null value correctly",
			Null{},
			[]byte("$-1\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			null := Null{}
			if got := null.Marshal(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Null.Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
