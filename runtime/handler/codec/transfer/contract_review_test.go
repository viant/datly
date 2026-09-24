package transfer

import (
	"context"
	"reflect"
	"testing"

	xcodec "github.com/viant/xdatly/codec"
)

func TestCodecPreservesDeclaredDestinationPointerDepth(t *testing.T) {
	type source struct{ ID int }
	type destination struct {
		ID int `transfer:"from=ID"`
	}
	base := reflect.TypeFor[destination]()
	for _, target := range []reflect.Type{base, reflect.PointerTo(base), reflect.PointerTo(reflect.PointerTo(base))} {
		t.Run(target.String(), func(t *testing.T) {
			codec, err := (Factory{}).New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[source](), DestinationType: target})
			if err != nil {
				t.Fatal(err)
			}
			value, err := codec.Value(context.Background(), source{ID: 7})
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(value) != target {
				t.Fatalf("codec returned %T, declared %s", value, target)
			}
		})
	}
}
