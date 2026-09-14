package openapi_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

type idCodec struct{}

func (idCodec) Value(_ context.Context, raw any, _ ...xcodec.Option) (any, error) {
	value, err := strconv.Atoi(raw.(string))
	return []int{value}, err
}

type idCodecFactory struct{ source, destination reflect.Type }

func (f *idCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.source = config.SourceType
	f.destination = config.DestinationType
	return idCodec{}, nil
}

func TestCodecSchemaUsesCompiledTransportSource(t *testing.T) {
	type input struct{ IDs []int }
	factory := &idCodecFactory{}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Codec"}, Routes: []*spec.Route{{Method: "GET", Path: "/codec"}},
		Parameters: []*spec.Parameter{{Name: "IDs", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "id"}, Codec: &spec.Codec{Body: "ids"}}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[envelope](), CodecFactory: factory})
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[string](), factory.source)
	require.Equal(t, reflect.TypeFor[[]int](), factory.destination)
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(_ context.Context, inv handler.Invocation) (any, error) {
		require.Equal(t, []int{7}, inv.Input.(*input).IDs)
		return &envelope{}, nil
	})})
	require.NoError(t, err)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	require.Equal(t, "string", doc.Paths["/codec"].Get.Parameters[0].Schema.Type)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", "/codec?id=7", nil))
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
}

func TestFormBodyAndOptionalWholeBody(t *testing.T) {
	type formInput struct {
		Label string `parameter:"Label,kind=form,in=label,required=true"`
		IDs   []int  `parameter:"IDs,kind=form,in=ids"`
	}
	entry := (fixture{input: reflect.TypeFor[formInput](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*formInput)
		require.Equal(t, "Example", input.Label)
		require.Equal(t, []int{3, 7}, input.IDs)
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	body := doc.Paths["/records"].Post.RequestBody
	require.True(t, body.Required)
	require.Contains(t, body.Content, "application/x-www-form-urlencoded")
	require.Equal(t, []string{"label"}, body.Content["application/x-www-form-urlencoded"].Schema.Required)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/records", strings.NewReader("label=Example&ids=3&ids=7"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	type optionalInput struct {
		Payload *patch `parameter:"Payload,kind=body"`
	}
	optional := (fixture{input: reflect.TypeFor[optionalInput]()}).registration(t)
	doc, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(optional))
	require.NoError(t, err)
	body = doc.Paths["/records"].Post.RequestBody
	require.False(t, body.Required)
	require.Len(t, body.Content["application/json"].Schema.AnyOf, 2)
}

func TestWithURIAbsoluteAndMultipleBaseRoutes(t *testing.T) {
	type relative struct {
		ID *int `parameter:"ID,kind=path,in=id,uri=/{id},required=true"`
	}
	type absolute struct {
		ID *int `parameter:"ID,kind=path,in=id,uri=/records/{id},required=true"`
	}
	for _, tc := range []struct {
		name  string
		input reflect.Type
		want  []string
	}{
		{"relative", reflect.TypeFor[relative](), []string{"/records", "/other", "/records/{id}", "/other/{id}"}},
		{"absolute", reflect.TypeFor[absolute](), []string{"/records", "/other", "/records/{id}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := (fixture{input: tc.input, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Routes"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}, {Method: "GET", Path: "/other"}}}}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			require.Len(t, doc.Paths, len(tc.want))
			for _, path := range tc.want {
				op := doc.Paths[path].Get
				if strings.Contains(path, "{id}") {
					require.Len(t, op.Parameters, 1)
					require.True(t, op.Parameters[0].Required)
				} else {
					require.Empty(t, op.Parameters)
				}
			}
		})
	}
}
