package input

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	bindinput "github.com/viant/bindly/input"
	bodyprovider "github.com/viant/bindly/provider/body"
	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
)

func TestStrictArgumentsOwnsNestedBodyAdmission(t *testing.T) {
	type row struct {
		ID int `json:"id"`
	}
	type payload struct {
		Rows []row `json:"rows"`
	}
	for _, strict := range []bool{false, true} {
		for _, name := range []string{"", "data"} {
			plan, err := NewCompiler(WithStrictArguments(strict)).Compile([]Argument{{PublicName: "payload", Source: bindstate.Location{Kind: requestprovider.BodyKind, In: name}, SourceType: reflect.TypeFor[payload]()}})
			require.NoError(t, err)
			scope, err := plan.Scope(Arguments{"payload": map[string]any{"rows": []any{map[string]any{"id": 11, "unexpected": true}}}})
			require.NoError(t, err)
			_, _, err = scope.Body().Locate(nil).Value(context.Background(), reflect.TypeFor[payload](), name)
			if strict {
				var invalid *bindinput.Error
				require.ErrorAs(t, err, &invalid)
				require.ErrorContains(t, invalid.Cause, `unknown field "unexpected"`)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, scope.Close())
		}
	}
	// The ordinary REST provider remains lenient under the existing default.
	source, err := bodyprovider.New([]byte(`{"rows":[{"id":11,"unexpected":true}]}`), "application/json", nil)
	require.NoError(t, err)
	_, found, err := source.Value(context.Background(), reflect.TypeFor[payload](), "")
	require.True(t, found)
	require.NoError(t, err)
}
