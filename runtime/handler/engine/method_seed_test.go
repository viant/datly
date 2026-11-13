package engine

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
	rhandler "github.com/viant/datly/runtime/handler"
)

// A trusted seed must not turn an unsupported Method shape into an exemption
// from ordinary binding. Keep the existing provider/codec as its owner.
func TestBoundMethodSeedDoesNotRetainDoublePointer(t *testing.T) {
	type input struct {
		Method **string
		Has    *struct{ Method bool } `setMarker:"true"`
	}
	method := "PATCH"
	pointer := &method
	seed := &input{Method: &pointer, Has: &struct{ Method bool }{true}}
	contract := testRouteInput(t, reflect.TypeFor[input](), bindly.BindingSpec{Path: "Method", Location: bindstate.Location{Kind: "http_request", In: "method"}})
	scope, err := request.New(httptest.NewRequest("POST", "/parent", nil))
	require.NoError(t, err)
	defer scope.Close()
	entered := false
	_, err = New().Execute(context.Background(), Request{Input: contract, BoundInput: seed, Scope: scope, Handler: rhandler.HandlerFunc(func(_ context.Context, v rhandler.Invocation) (any, error) { entered = true; return v.Input, nil })})
	if err != nil {
		require.False(t, entered, "unsupported conversion must fail before business execution")
		return
	}
	require.True(t, entered)
	require.NotNil(t, seed.Method)
	require.NotNil(t, *seed.Method)
	require.Equal(t, "POST", **seed.Method, "double pointer must use ordinary transport binding")
}
