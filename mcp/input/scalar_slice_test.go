package input

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	bindstate "github.com/viant/bindly/state"
)

type customScalarSlice []int

func (*customScalarSlice) UnmarshalJSON([]byte) error { return nil }

type customScalarInteger int

func (*customScalarInteger) UnmarshalJSON([]byte) error { return nil }

type textScalarSlice []int

func (textScalarSlice) UnmarshalText([]byte) error { return nil }

type pointerTextScalarSlice []int

func (*pointerTextScalarSlice) UnmarshalText([]byte) error { return nil }

type textScalarInteger int

func (textScalarInteger) UnmarshalText([]byte) error { return nil }

type pointerTextScalarInteger int

func (*pointerTextScalarInteger) UnmarshalText([]byte) error { return nil }

func TestScalarSliceAdmissionUsesCanonicalBindlyQuery(t *testing.T) {
	type input struct{ IDs []int }
	location := bindstate.Location{Kind: "query", In: "ids"}
	plan, err := NewCompiler(WithStrictArguments(true)).Compile([]Argument{{PublicName: "ids", Aliases: []string{"legacy_ids"}, Source: location, SourceType: reflect.TypeFor[[]int](), AcceptScalar: true}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		args Arguments
		want []int
	}{
		{"scalar", Arguments{"ids": 7}, []int{7}},
		{"zero", Arguments{"ids": 0}, []int{0}},
		{"alias", Arguments{"legacy_ids": 7}, []int{7}},
		{"array", Arguments{"ids": []int{7, 9}}, []int{7, 9}},
		{"empty", Arguments{"ids": []int{}}, []int{}},
		{"omitted", Arguments{}, nil},
		{"null", Arguments{"ids": nil}, nil},
		{"empty string retains omission", Arguments{"ids": ""}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := plan.Scope(tc.args)
			require.NoError(t, err)
			defer scope.Close()
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			require.NoError(t, err)
			optional := false
			binding, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "IDs", Location: location, SourceType: reflect.TypeFor[[]int](), Required: &optional})
			require.NoError(t, err)
			var actual input
			require.NoError(t, injector.Bind(t.Context(), &actual, bindly.WithPlan(binding)))
			require.Equal(t, tc.want, actual.IDs)
		})
	}
	for _, invalid := range []any{"7", true, 1.5, json.Number("9223372036854775808"), map[string]any{"id": 7}, []any{7, "9"}} {
		_, err := plan.Scope(Arguments{"ids": invalid})
		require.Error(t, err, "admitted %#v", invalid)
		var inputError *bindinput.Error
		require.True(t, errors.As(err, &inputError), "%T: %v", err, err)
	}
	_, err = plan.Scope(Arguments{"ids": 7, "legacy_ids": 7})
	require.ErrorContains(t, err, "conflicting")
	_, err = plan.Scope(Arguments{"unknown": 7})
	require.ErrorContains(t, err, "unknown")
	ordinary, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: location, SourceType: reflect.TypeFor[[]int]()}})
	require.NoError(t, err)
	_, err = ordinary.Scope(Arguments{"ids": 7})
	require.Error(t, err, "untagged numeric slice admitted scalar")
}

func TestScalarSliceAdmissionRejectsUnsupportedContracts(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[*[]int](), reflect.TypeFor[[]string](), reflect.TypeFor[[]float64](), reflect.TypeFor[[]byte](), reflect.TypeFor[[]struct{ ID int }]()} {
		_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "ids"}, SourceType: typ, AcceptScalar: true}})
		require.ErrorContains(t, err, "signed integer query or path slice")
	}
	for _, kind := range []string{"body", "header", "cookie", "form"} {
		_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: kind, In: "ids"}, SourceType: reflect.TypeFor[[]int](), AcceptScalar: true}})
		require.ErrorContains(t, err, "signed integer query or path slice")
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[customScalarSlice](), reflect.TypeFor[[]customScalarInteger]()} {
		_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "ids"}, SourceType: typ, AcceptScalar: true}})
		require.ErrorContains(t, err, "custom JSON or text decoding")
	}
}

func TestScalarSliceAdmissionUsesCanonicalBindlyPath(t *testing.T) {
	type input struct{ IDs []int }
	location := bindstate.Location{Kind: "path", In: "ids"}
	for _, strict := range []bool{false, true} {
		plan, err := NewCompiler(WithStrictArguments(strict)).Compile([]Argument{{PublicName: "ids", Aliases: []string{"legacy_ids"}, Source: location, SourceType: reflect.TypeFor[[]int](), AcceptScalar: true}})
		require.NoError(t, err)
		for _, tc := range []struct {
			name string
			args Arguments
			want []int
		}{
			{"scalar", Arguments{"ids": 7}, []int{7}},
			{"zero", Arguments{"ids": 0}, []int{0}},
			{"negative", Arguments{"ids": -7}, []int{-7}},
			{"alias", Arguments{"legacy_ids": 7}, []int{7}},
			{"array", Arguments{"ids": []int{7, 9}}, []int{7, 9}},
			{"empty array", Arguments{"ids": []int{}}, []int{}},
			{"omitted", Arguments{}, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				scope, err := plan.Scope(tc.args)
				require.NoError(t, err)
				defer scope.Close()
				injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
				require.NoError(t, err)
				required := true
				binding, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "IDs", Location: location, SourceType: reflect.TypeFor[[]int](), Required: &required})
				require.NoError(t, err)
				var actual input
				err = injector.Bind(t.Context(), &actual, bindly.WithPlan(binding))
				if len(tc.want) == 0 {
					var inputError *bindinput.Error
					require.ErrorAs(t, err, &inputError)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, actual.IDs)
			})
		}
		for _, invalid := range []any{"7", "", true, 1.5, json.Number("9223372036854775808"), map[string]any{"id": 7}, []any{7, "9"}} {
			_, err := plan.Scope(Arguments{"ids": invalid})
			require.Error(t, err, "admitted %#v", invalid)
			var inputError *bindinput.Error
			require.ErrorAs(t, err, &inputError)
			var cause *json.UnmarshalTypeError
			require.ErrorAs(t, err, &cause)
		}
		_, err = plan.Scope(Arguments{"ids": nil})
		require.ErrorContains(t, err, "non-null array")
		_, err = plan.Scope(Arguments{"ids": 7, "legacy_ids": 7})
		require.ErrorContains(t, err, "conflicting")
		scope, err := plan.Scope(Arguments{"ids": 7, "unknown": true})
		if strict {
			require.ErrorContains(t, err, "unknown")
		} else {
			require.NoError(t, err)
			scope.Close()
		}
	}
	ordinary, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: location, SourceType: reflect.TypeFor[[]int]()}})
	require.NoError(t, err)
	_, err = ordinary.Scope(Arguments{"ids": 7})
	require.Error(t, err, "untagged path slice admitted scalar")
}

func TestScalarSliceAdmissionRejectsTextDecoderOwnership(t *testing.T) {
	// encoding/json delegates quoted JSON strings to TextUnmarshaler even
	// when the Go kind is integer. Discovery must not advertise integer-only
	// admission while a custom decoder owns a different wire contract.
	var decoded pointerTextScalarInteger
	require.NoError(t, json.Unmarshal([]byte(`"custom"`), &decoded))
	for _, typ := range []reflect.Type{reflect.TypeFor[textScalarSlice](), reflect.TypeFor[pointerTextScalarSlice](), reflect.TypeFor[[]textScalarInteger](), reflect.TypeFor[[]pointerTextScalarInteger]()} {
		t.Run(typ.String(), func(t *testing.T) {
			_, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: bindstate.Location{Kind: "query", In: "ids"}, SourceType: typ, AcceptScalar: true}})
			require.ErrorContains(t, err, "custom JSON or text decoding")
		})
	}
}

func TestScalarSliceRequiredNullUsesNativeRequiredBinding(t *testing.T) {
	type input struct{ IDs []int }
	location := bindstate.Location{Kind: "query", In: "ids"}
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "ids", Source: location, SourceType: reflect.TypeFor[[]int](), AcceptScalar: true}})
	require.NoError(t, err)
	for _, args := range []Arguments{{}, {"ids": nil}, {"ids": ""}} {
		scope, err := plan.Scope(args)
		require.NoError(t, err)
		injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
		require.NoError(t, err)
		required := true
		binding, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "IDs", Location: location, SourceType: reflect.TypeFor[[]int](), Required: &required})
		require.NoError(t, err)
		var actual input
		err = injector.Bind(t.Context(), &actual, bindly.WithPlan(binding))
		require.Error(t, err)
		var inputError *bindinput.Error
		require.ErrorAs(t, err, &inputError)
		scope.Close()
	}
}

func TestScalarSliceAdmissionPreservesSignedIntegerWidths(t *testing.T) {
	for _, kind := range []string{"query", "path"} {
		for _, typ := range []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64]()} {
			argument := Argument{PublicName: "ids", Source: bindstate.Location{Kind: kind, In: "ids"}, SourceType: reflect.SliceOf(typ), AcceptScalar: true}
			plan, err := NewCompiler().Compile([]Argument{argument})
			require.NoError(t, err)
			for _, number := range []int{0, -1, 127} {
				resolved, found, err := (Arguments{"ids": number}).resolve(plan.arguments[0])
				require.NoError(t, err)
				require.True(t, found)
				array, _, err := (Arguments{"ids": []int{number}}).resolve(plan.arguments[0])
				require.NoError(t, err)
				require.Equal(t, array.wire, resolved.wire)
			}
		}
	}
	_, err := decodeSourceValue([]byte("128"), reflect.TypeFor[[]int8](), true)
	require.Error(t, err)
}
