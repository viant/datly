package transfer

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	xcodec "github.com/viant/xdatly/codec"
)

type testNestedCodec struct{}

func (testNestedCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	return raw.(string) + "!", nil
}

func TestOriginalTransferTagsCompileIntoTransformCodec(t *testing.T) {
	type source struct {
		TimeTaken int
		BarName   string
		BarID     int64
	}
	type nested struct {
		ID int64 `transfer:"from=BarID"`
	}
	type destination struct {
		Elapsed time.Duration `transfer:"from=TimeTaken"`
		Name    string        `transfer:"from=BarName,codec=suffix"`
		Context *nested
	}
	factory := Factory{Codecs: map[string]xcodec.Instance{"suffix": testNestedCodec{}}}
	instance, err := factory.New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[source](), DestinationType: reflect.TypeFor[destination]()})
	if err != nil {
		t.Fatal(err)
	}
	input := source{TimeTaken: int(time.Second), BarName: "Ada", BarID: 9007199254740993}
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			value, err := instance.Value(context.Background(), input)
			if err != nil {
				t.Errorf("Value: %v", err)
				return
			}
			actual := value.(destination)
			if actual.Elapsed != time.Second || actual.Name != "Ada!" || actual.Context == nil || actual.Context.ID != input.BarID {
				t.Errorf("transformed result: %#v", actual)
			}
		}()
	}
	wait.Wait()
	if _, err := instance.Value(context.Background(), &struct{ Different string }{}); err == nil {
		t.Fatal("first source shape was reused for an unrelated type")
	}
}

func TestTransformCodecRejectsUnknownNestedCodecAndBadSource(t *testing.T) {
	type source struct{ Name string }
	type destination struct {
		Name string `transfer:"from=Name,codec=missing"`
	}
	_, err := (Factory{}).New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[source](), DestinationType: reflect.TypeFor[destination]()})
	if err == nil {
		t.Fatal("unregistered nested codec was ignored")
	}
	type invalid struct {
		Name string `transfer:"from=Missing"`
	}
	_, err = (Factory{}).New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[source](), DestinationType: reflect.TypeFor[invalid]()})
	if err == nil {
		t.Fatal("unknown source path was accepted")
	}
}

func TestPositionalTransferTag(t *testing.T) {
	type source struct{ Name string }
	type destination struct {
		Name string `transfer:"Name"`
	}
	codec, err := (Factory{}).New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[source](), DestinationType: reflect.TypeFor[destination]()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := codec.Value(context.Background(), source{Name: "Ada"})
	if err != nil || value.(destination).Name != "Ada" {
		t.Fatalf("positional transfer value=%#v err=%v", value, err)
	}
}

func TestFactorySelectedByTypeAcceptsAliasedCodecBody(t *testing.T) {
	type source struct{ Name string }
	type destination struct {
		Name string `transfer:"Name"`
	}
	codec, err := (Factory{}).New(&xcodec.Config{Body: "alias.Factory", SourceType: reflect.TypeFor[source](), DestinationType: reflect.TypeFor[destination]()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := codec.Value(context.Background(), source{Name: "Ada"})
	if err != nil || value.(destination).Name != "Ada" {
		t.Fatalf("aliased codec value=%#v err=%v", value, err)
	}
}

func TestCodecSelectsDynamicMapArrayWithoutLosingIntegerPrecision(t *testing.T) {
	type destination struct {
		ID int64 `transfer:"from=items[0].id"`
	}
	codec, err := (Factory{}).New(&xcodec.Config{
		Body: "alias.Factory", SourceType: reflect.TypeFor[map[string]any](), DestinationType: reflect.TypeFor[destination](),
	})
	if err != nil {
		t.Fatal(err)
	}
	source := map[string]any{"items": []any{map[string]any{"id": json.Number("9007199254740993")}}}
	value, err := codec.Value(context.Background(), source)
	if err != nil || value.(destination).ID != 9007199254740993 {
		t.Fatalf("dynamic transfer value=%#v err=%v", value, err)
	}
}
