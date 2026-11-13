package input

import (
	"errors"
	bindinput "github.com/viant/bindly/input"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	bindstate "github.com/viant/bindly/state"
)

func TestAuthoredNullPolicyIsPreparedAndOwned(t *testing.T) {
	nullable := false
	arguments := []Argument{{PublicName: "id", Aliases: []string{"legacy"}, Nullable: &nullable, Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeFor[*int]()}}
	plan, err := NewCompiler().Compile(arguments)
	require.NoError(t, err)
	nullable = true
	exported := plan.Arguments()
	*exported[0].Nullable = true
	for _, name := range []string{"id", "legacy"} {
		_, err = plan.Scope(Arguments{name: nil})
		require.ErrorContains(t, err, `MCP argument "id"`)
		var bindingError *bindinput.Error
		require.True(t, errors.As(err, &bindingError))
		require.Equal(t, 400, bindingError.StatusCode())
	}
	_, err = plan.Scope(Arguments{})
	require.NoError(t, err)
	_, err = plan.Scope(Arguments{"id": 0})
	require.NoError(t, err)
}

type countedJSONValue struct{ calls *int }

func (v countedJSONValue) MarshalJSON() ([]byte, error) { *v.calls++; return []byte("1"), nil }
func TestAuthoredNullPolicyReusesJSONEncoding(t *testing.T) {
	nullable := false
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "id", Nullable: &nullable, Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeFor[*int]()}})
	require.NoError(t, err)
	calls := 0
	_, err = plan.Scope(Arguments{"id": countedJSONValue{calls: &calls}})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
