package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/response"
)

type failingContractInput struct{}
type failingContractOutput struct{ Data []string }

func (*failingContractOutput) Contract(context.Context, bindly.Binder) (*contract.Policy, error) {
	return nil, &response.Error{Code: 401, Payload: map[string]any{"status": "error", "message": "credentials required", "data": []any{}}, Cause: errors.New("private verifier detail")}
}
func TestContractFailureRetainsDeclaredPublicErrorBody(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ContractFailure"}, Routes: []*spec.Route{{Method: "GET", Path: "/contract-failure"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[failingContractInput](), OutputType: reflect.TypeFor[failingContractOutput]()})
	require.NoError(t, err)
	calls := 0
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(context.Context, *failingContractInput) (*failingContractOutput, error) {
		calls++
		return &failingContractOutput{}, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	rec := httptest.NewRecorder()
	NewHandler(runtime, nil, "test").ServeHTTP(rec, httptest.NewRequest("GET", "/contract-failure", nil))
	require.Equal(t, 401, rec.Code)
	require.JSONEq(t, `{"status":"error","message":"credentials required","data":[]}`, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "private verifier detail")
	require.Zero(t, calls)
}
