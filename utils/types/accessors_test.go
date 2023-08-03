package types

import (
	"net/http"
	"reflect"
	"testing"
)

func TestAccessors(t *testing.T) {
	testCases := []struct {
		description string
		namer       Namer
		rType       reflect.Type
	}{
		{
			description: "cyclic ref",
			namer:       &VeltyNamer{},
			rType:       reflect.TypeOf(&http.Request{}),
		},
	}

	for _, testCase := range testCases {
		accessor := NewAccessors(testCase.namer)
		accessor.Init(testCase.rType)
	}
}
