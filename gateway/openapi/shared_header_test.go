package openapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
)

func TestCompatibleCaseFoldedHeaderConsumers(t *testing.T) {
	type input struct {
		A string `parameter:"A,kind=header,in=X-Name"`
		B string `parameter:"B,kind=header,in=x-name"`
	}
	var received *input
	entry := (fixture{input: reflect.TypeFor[input](), execute: func(_ context.Context, invocation handler.Invocation) (any, error) {
		received = invocation.Input.(*input)
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	parameters := doc.Paths["/records"].Post.Parameters
	require.Len(t, parameters, 1, "one request header has two compatible consumers")
	require.Equal(t, "header", parameters[0].In)
	require.Equal(t, "X-Name", parameters[0].Name)
	require.Equal(t, "string", parameters[0].Schema.Type)

	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/records", nil)
	request.Header.Set("x-name", "shared-value")
	response := httptest.NewRecorder()
	gatewayhttp.NewHandler(runtime, nil, "test").ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotNil(t, received)
	require.Equal(t, "shared-value", received.A)
	require.Equal(t, "shared-value", received.B)
}

func TestIncompatibleCaseFoldedHeaderConsumersFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input reflect.Type
	}{
		{"wire type", reflect.TypeFor[struct {
			A string `parameter:"A,kind=header,in=X-Name"`
			B int    `parameter:"B,kind=header,in=x-name"`
		}]()},
		{"default", reflect.TypeFor[struct {
			A string `parameter:"A,kind=header,in=X-Name,value='first'"`
			B string `parameter:"B,kind=header,in=x-name,value='second'"`
		}]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := (fixture{input: tc.input}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.ErrorContains(t, err, "conflicting transitive transport input header:x-name")
			require.Nil(t, doc)
		})
	}
}
