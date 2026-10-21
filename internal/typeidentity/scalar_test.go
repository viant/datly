package typeidentity

import (
	"github.com/stretchr/testify/require"
	"go/token"
	"go/types"
	"reflect"
	"testing"
)

type scalarName string
type otherScalarName string
type scalarCount int
type scalarJSON string

func (scalarJSON) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

func TestReflectScalarPair(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wire, dest reflect.Type
		schema     string
		nullable   bool
	}{
		{"string", reflect.TypeFor[*string](), reflect.TypeFor[string](), "string", true},
		{"bool inverse", reflect.TypeFor[bool](), reflect.TypeFor[*bool](), "boolean", false},
		{"int", reflect.TypeFor[*int](), reflect.TypeFor[int](), "integer", true},
		{"named string", reflect.TypeFor[*scalarName](), reflect.TypeFor[scalarName](), "string", true},
		{"named int", reflect.TypeFor[scalarCount](), reflect.TypeFor[*scalarCount](), "integer", false},
		{"distinct named", reflect.TypeFor[*scalarName](), reflect.TypeFor[otherScalarName](), "", false},
		{"width", reflect.TypeFor[*int32](), reflect.TypeFor[int64](), "", false},
		{"same", reflect.TypeFor[int](), reflect.TypeFor[int](), "", false},
		{"deep", reflect.TypeFor[**int](), reflect.TypeFor[*int](), "", false},
		{"float", reflect.TypeFor[*float64](), reflect.TypeFor[float64](), "", false},
		{"list", reflect.TypeFor[*[]int](), reflect.TypeFor[[]int](), "", false},
		{"serializer", reflect.TypeFor[*scalarJSON](), reflect.TypeFor[scalarJSON](), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ReflectScalarPair(tc.wire, tc.dest)
			require.Equal(t, tc.schema != "", ok)
			if ok {
				require.Equal(t, ScalarPair{tc.schema, tc.nullable}, got)
			}
		})
	}
}

func TestSourceScalarPairCanonicalIdentity(t *testing.T) {
	pkg := types.NewPackage("example.com/types", "model")
	named := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Name", nil), types.Typ[types.String], nil)
	other := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Other", nil), types.Typ[types.String], nil)
	alias := types.NewAlias(types.NewTypeName(token.NoPos, pkg, "Alias", nil), named)
	pair, ok := SourceScalarPair(types.NewPointer(alias), named)
	require.True(t, ok)
	require.Equal(t, ScalarPair{"string", true}, pair)
	_, ok = SourceScalarPair(types.NewPointer(named), other)
	require.False(t, ok)
	_, ok = SourceScalarPair(types.NewPointer(types.Typ[types.Int32]), types.Typ[types.Int64])
	require.False(t, ok)
}

type scalarBusiness string

func (scalarBusiness) MarshalJSON() string { return "business" }
func TestScalarPairSerializerSignatureIdentity(t *testing.T) {
	_, ok := ReflectScalarPair(reflect.TypeFor[*scalarBusiness](), reflect.TypeFor[scalarBusiness]())
	require.True(t, ok)
	pkg := types.NewPackage("example.com/types", "model")
	for _, serializer := range []bool{false, true} {
		named := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Named", nil), types.Typ[types.String], nil)
		results := types.NewTuple(types.NewVar(token.NoPos, nil, "", types.Typ[types.String]))
		if serializer {
			results = types.NewTuple(types.NewVar(token.NoPos, nil, "", types.NewSlice(types.Typ[types.Byte])), types.NewVar(token.NoPos, nil, "", types.Universe.Lookup("error").Type()))
		}
		signature := types.NewSignatureType(types.NewVar(token.NoPos, nil, "", named), nil, nil, types.NewTuple(), results, false)
		named.AddMethod(types.NewFunc(token.NoPos, pkg, "MarshalJSON", signature))
		_, ok := SourceScalarPair(types.NewPointer(named), named)
		require.Equal(t, !serializer, ok)
	}
}
