package serde

import (
	"reflect"
	"testing"
)

func TestError_Marshal(t *testing.T) {
	type fields struct {
		value string
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			"Should marshal an error correctly",
			fields{"ECHO FOO"},
			[]byte("-ECHO FOO\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorString := Error{
				Value: tt.fields.value,
			}
			if got := errorString.Marshal(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Error.Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
