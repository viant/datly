package bootstrap

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

type fallbackCodec struct{}

func (fallbackCodec) Value(context.Context, interface{}, ...xcodec.Option) (interface{}, error) {
	return nil, nil
}

type fallbackCodecFactory struct {
	called bool
	config *xcodec.Config
}

func (f *fallbackCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.called = true
	f.config = config
	return fallbackCodec{}, nil
}

func TestCodecFactoryProvidesStructQLAndDelegatesCustomCodecs(t *testing.T) {
	type event struct{ ID int }
	type helper struct{ Values []int }
	fallback := &fallbackCodecFactory{}
	factory := newCodecFactory(fallback)
	if _, err := factory.New(&xcodec.Config{
		Body: "STRUCTQL", SourceType: reflect.TypeOf([]event{}), DestinationType: reflect.TypeOf(helper{}),
		Args: []string{"SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1"},
	}); err != nil || fallback.called {
		t.Fatalf("built-in StructQL = %v, fallback called = %v", err, fallback.called)
	}
	if _, err := factory.New(&xcodec.Config{Body: "custom"}); err != nil || !fallback.called {
		t.Fatalf("custom codec = %v, fallback called = %v", err, fallback.called)
	}
}

func TestBuildArtifactPreservesPackageCodecSourceAndDestinationTypes(t *testing.T) {
	type input struct {
		Values []string `bind:"Values,kind=query,in=value,dataType=string" codec:"custom"`
	}
	factory := &fallbackCodecFactory{}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/values"}}},
		InputType: reflect.TypeOf(input{}), CodecFactory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact == nil || len(artifact.Component.Parameters) != 1 || artifact.Component.Parameters[0].TypeExpr != "string" {
		t.Fatalf("artifact = %+v", artifact)
	}
	if factory.config == nil || factory.config.SourceType != reflect.TypeOf("") || factory.config.DestinationType != reflect.TypeOf([]string{}) {
		t.Fatalf("codec config = %+v", factory.config)
	}
}
