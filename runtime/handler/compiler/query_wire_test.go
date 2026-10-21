package compiler

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bs "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type namedFormString string
type namedQueryBool bool

type boolWireTransformer struct{}

func (boolWireTransformer) Transform(_ context.Context, _ locator.Resolver, input any) (any, error) {
	return input, nil
}

func TestExplicitBoolWirePreservesCompetingAuthority(t *testing.T) {
	field := reflect.StructField{Type: reflect.TypeFor[bool]()}
	for _, b := range []bindly.BindingSpec{
		{DataType: "*bool", Location: bs.Location{Kind: "query"}, SourceType: reflect.TypeFor[bool]()},
		{DataType: "*bool", Location: bs.Location{Kind: "query"}, Transformer: boolWireTransformer{}},
	} {
		require.NoError(t, applyExplicitScalarWire(field, &b))
		require.Nil(t, b.Extension)
	}
	p := &spec.Parameter{WireSchema: &spec.WireSchema{Type: "boolean", Nullable: false}}
	b := bindly.BindingSpec{DataType: "*bool", Location: bs.Location{Kind: "query"}, Extension: p}
	require.NoError(t, applyExplicitScalarWire(field, &b))
	require.Same(t, p, b.Extension)
	require.False(t, p.WireSchema.Nullable)
}
