package output

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
)

func TestSelectedJSONPreservesInternalFieldExclusion(t *testing.T) {
	type record struct {
		ID     int    `json:"id"`
		Secret string `json:"secret" internal:"true"`
		Name   string `json:"name"`
	}
	type envelope struct {
		Data []*record `json:"data"`
	}
	value := &envelope{Data: []*record{{ID: 7, Secret: "private-value", Name: "Example"}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	for _, fields := range [][]string{{"ID"}, {"ID", "Secret"}} {
		t.Run(fields[len(fields)-1], func(t *testing.T) {
			filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Data"}, Fields: fields}})
			require.NoError(t, err)
			ctx := dexec.CaptureOutputSelection(context.Background())
			scoped, complete := dexec.ScopeOutputSelection(ctx)
			dexec.BeginOutputSelection(scoped)
			dexec.PublishOutputSelection(scoped, value, filter)
			complete(value, nil)
			encoded, err := plan.Encode(ctx, "json", value)
			require.NoError(t, err)
			require.JSONEq(t, `{"data":[{"id":7}]}`, string(encoded.Data))
			require.Equal(t, "private-value", value.Data[0].Secret)
		})
	}
}
