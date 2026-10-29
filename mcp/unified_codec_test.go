package mcp

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/datly/bootstrap"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
)

type codecInput struct {
	IDs []int `json:"ids"`
}

type codecOutput struct {
	HandlerIDs []int `json:"handlerIDs"`
	BoundIDs   []int `json:"boundIDs"`
}

type countingCodec struct {
	calls int
	raw   interface{}
}

func (c *countingCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	c.calls++
	c.raw = raw
	value, err := strconv.Atoi(raw.(string))
	if err != nil {
		return nil, err
	}
	return []int{value}, nil
}

type countingCodecFactory struct {
	codec *countingCodec
}

func (f countingCodecFactory) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	return f.codec, nil
}

func TestToolCodecRunsOnceAndOutputBindingUsesCanonicalInput(t *testing.T) {
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Codec"}, Name: "Codec",
		Routes: []*spec.Route{{Method: "POST", Path: "/codec", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "codec.run"}}}},
		Parameters: []*spec.Parameter{{
			Name: "IDs", Source: spec.BindSource{Kind: "query", Name: "ids"}, Required: &required,
			TypeExpr: "string", OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: "AsInts"},
		}},
	}
	codec := &countingCodec{}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(codecInput{}), OutputType: reflect.TypeOf(codecOutput{}),
		CodecFactory: countingCodecFactory{codec: codec},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := customhandler.New[codecInput, codecOutput](xhandler.ContractFunc[codecInput, codecOutput](
		func(ctx context.Context, session xhandler.Session, input *codecInput, output *codecOutput) error {
			output.HandlerIDs = append([]int(nil), input.IDs...)
			projection := struct {
				IDs []int `parameter:"IDs,kind=input,in=IDs"`
			}{}
			if err := session.Binder().Bind(ctx, &projection); err != nil {
				return err
			}
			output.BoundIDs = projection.IDs
			return nil
		},
	))
	result := executeRuntimeTool(t, &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(codecOutput{}), Handler: handler,
	}, "codec.run", map[string]interface{}{"ids": "7"})
	if codec.calls != 1 || codec.raw != "7" {
		t.Fatalf("codec calls=%d raw=%#v", codec.calls, codec.raw)
	}
	for _, field := range []string{"handlerIDs", "boundIDs"} {
		values, ok := testharness.StructuredObject(t, result.StructuredContent)[field].([]interface{})
		if !ok || len(values) != 1 || values[0] != float64(7) {
			t.Fatalf("%s = %#v; result=%+v", field, testharness.StructuredObject(t, result.StructuredContent)[field], result.StructuredContent)
		}
	}
}
