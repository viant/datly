package expand

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
)

type renderGroupPredicateHandler struct{}

func (r *renderGroupPredicateHandler) Compute(_ context.Context, value interface{}) (*codec.Criteria, error) {
	return &codec.Criteria{Expression: "t.ACCOUNT_ID = ?", Placeholders: []interface{}{value}}, nil
}

func TestPredicateBuilder_NilReceiver(t *testing.T) {
	var builder *PredicateBuilder

	got := builder.CombineOr("x = ?").Build("WHERE")
	if got == "" {
		t.Fatalf("expected combined predicate, got empty string")
	}

	got = builder.And().CombineAnd("y = ?").Build("WHERE")
	if got == "" {
		t.Fatalf("expected predicate after And on nil receiver, got empty string")
	}
}

func TestPredicateRenderGroup_DoesNotMutateExpansionState(t *testing.T) {
	type input struct {
		AccountID int
	}
	stateType := structology.NewStateType(reflect.TypeOf(input{}))
	parameterState := stateType.NewState()
	require.NoError(t, parameterState.SetInt("AccountID", 29))
	dataUnit := NewDataUnit(nil)
	expandContext := &Context{Context: context.Background(), DataUnit: dataUnit}
	predicate := NewPredicate(expandContext, parameterState, []*PredicateConfig{{
		Group:    7,
		Selector: stateType.Lookup("AccountID"),
		Expander: &renderGroupPredicateHandler{},
	}}, stateType)

	actual, err := predicate.RenderGroup(7, "AND")
	require.NoError(t, err)
	assert.Equal(t, "(t.ACCOUNT_ID = ?)", actual.Expression)
	assert.Equal(t, []interface{}{29}, actual.Args)
	assert.Empty(t, dataUnit.ParamsGroup)
	assert.Empty(t, expandContext.Filters)
}

//func TestPredicate(t *testing.T) {
//	type Foo struct {
//		ID       int
//		Name     string
//		Quantity float64
//	}
//
//	type FooHas struct {
//		ID       bool
//		Name     bool
//		Quantity bool
//	}
//
//	testCases := []struct {
//		state  interface{}
//		has    interface{}
//		config []*expand.PredicateConfig
//	}{
//		{
//			state: &Foo{
//				ID:   15,
//				Name: "abc",
//			},
//			has: &FooHas{
//				ID:   true,
//				Name: true,
//			},
//			config: []*expand.PredicateConfig{
//				{
//					NormalizeObject:       0,
//					StateAccessor: types.NewAccessor(xunsafe.FieldByName(reflect.TypeOf(Foo{}), "ID")),
//					HasAccessor:   types.NewAccessor(xunsafe.FieldByName(reflect.TypeOf(FooHas{}), "ID")),
//					Expander: func(ctx *expand.NormalizeObject, value interface{}) (*parameter.Criteria, error) {
//						return &parameter.Criteria{
//							Query: "ID = ?",
//							Args:  []interface{}{value},
//						}, nil
//					},
//				},
//			},
//		},
//	}
//
//	for _, testCase := range testCases {
//		predicate := expand.NewPredicate(nil, testCase.state, testCase.has, testCase.config)
//
//		result, err := predicate.Expand(0)
//		fmt.Print(result, err)
//	}
//}
