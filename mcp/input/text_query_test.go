package input

import (
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
)

func TestTextQueryAdmissionRetainsQueryWireAndConversion(t *testing.T) {
	location := bindstate.Location{Kind: "query", In: "id"}
	argument := Argument{PublicName: "ids", Aliases: []string{"legacy_ids"}, Source: location, SourceType: reflect.TypeFor[[]int](), AcceptText: true}
	type input struct {
		IDs []int
		Has *struct{ IDs bool } `setMarker:"true"`
	}
	bind := func(scope *requestprovider.Scope, required bool) (input, error) {
		injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
		require.NoError(t, err)
		plan, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "IDs", Location: location, SourceType: argument.SourceType, Required: &required})
		require.NoError(t, err)
		var result input
		err = injector.Bind(t.Context(), &result, bindly.WithPlan(plan))
		return result, err
	}
	for _, strict := range []bool{false, true} {
		plan, err := NewCompiler(WithStrictArguments(strict)).Compile([]Argument{argument})
		require.NoError(t, err)
		for _, text := range []string{"50", "050", "+50", "-50", "0", "1e2", " 50", "50 ", "", " ", "50,51", "9223372036854775808", "invalid"} {
			t.Run(strconv.FormatBool(strict)+"/"+text, func(t *testing.T) {
				wire, found, err := (Arguments{"ids": []any{7, text, 9}}).resolve(plan.arguments[0])
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, []string{"7", text, "9"}, wire.wire)
				native := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"id": {"7", text, "9"}}))
				defer native.Close()
				want, wantErr := bind(native, false)
				t.Logf("bare Bindly query conversion text=%q bound=%v error=%v", text, want.IDs, wantErr)
				scope, err := plan.Scope(Arguments{"legacy_ids": []any{7, text, 9}})
				require.NoError(t, err)
				defer scope.Close()
				got, gotErr := bind(scope, false)
				require.Equal(t, want, got)
				require.Equal(t, wantErr == nil, gotErr == nil)
				if wantErr != nil {
					var cause *strconv.NumError
					require.ErrorAs(t, wantErr, &cause)
					var actualCause *strconv.NumError
					require.ErrorAs(t, gotErr, &actualCause)
					require.Equal(t, cause, actualCause)
					var inputErr *bindinput.Error
					require.ErrorAs(t, gotErr, &inputErr)
				}
			})
		}
		for _, args := range []Arguments{{}, {"ids": nil}, {"ids": []int(nil)}, {"ids": []any{}}, {"ids": []any{7, "9"}}, {"ids": []any{7}, "unknown": true}} {
			scope, err := plan.Scope(args)
			if strict && args["unknown"] != nil {
				require.ErrorContains(t, err, "unknown")
				continue
			}
			require.NoError(t, err)
			got, err := bind(scope, false)
			require.NoError(t, err)
			if _, present := args["ids"]; !present || args["ids"] == nil {
				require.Nil(t, got.IDs)
			}
			scope.Close()
		}
		for _, args := range []Arguments{{}, {"ids": nil}, {"ids": []int(nil)}, {"ids": []any{}}} {
			scope, err := plan.Scope(args)
			require.NoError(t, err)
			_, err = bind(scope, true)
			require.Error(t, err)
			scope.Close()
		}
		for _, invalid := range []any{"50", 50, true, 1.5, []any{true}, []any{1.5}, []any{nil}, []any{map[string]any{"id": 7}}, []any{json.Number("9223372036854775808")}} {
			_, err := plan.Scope(Arguments{"ids": invalid})
			require.Error(t, err, "%#v", invalid)
			var inputErr *bindinput.Error
			require.ErrorAs(t, err, &inputErr)
		}
		_, err = plan.Scope(Arguments{"ids": []int{7}, "legacy_ids": []int{7}})
		require.ErrorContains(t, err, "conflicting")
	}
}

func TestTextQueryAdmissionRestrictionsAndDefault(t *testing.T) {
	for _, kind := range []string{"path", "body", "form", "header", "cookie"} {
		_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: kind, In: "id"}, SourceType: reflect.TypeFor[[]int](), AcceptText: true}})
		require.ErrorContains(t, err, "text admission requires")
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[*[]int](), reflect.TypeFor[[]string](), reflect.TypeFor[[]float64](), reflect.TypeFor[[]uint64](), reflect.TypeFor[customScalarSlice](), reflect.TypeFor[[]pointerTextScalarInteger]()} {
		_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: typ, AcceptText: true}})
		require.Error(t, err)
	}
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeFor[[]int]()}})
	require.NoError(t, err)
	_, err = plan.Scope(Arguments{"ids": []any{7, "9"}})
	require.Error(t, err)
}

var textQuerySourceFault = errors.New("query source fault")

type failingTextQuerySource struct{}

func (failingTextQuerySource) MarshalJSON() ([]byte, error) { return nil, textQuerySourceFault }
func TestTextQueryAdmissionPreservesSourceFault(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeFor[[]int](), AcceptText: true}})
	require.NoError(t, err)
	_, err = plan.Scope(Arguments{"ids": failingTextQuerySource{}})
	require.ErrorIs(t, err, textQuerySourceFault)
}
