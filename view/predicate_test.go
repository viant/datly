package view

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/view/extension"
)

func TestPredicateCacheKeyIncludesOwningStateType(t *testing.T) {
	stringType := reflect.TypeOf("")
	first := predicateKey{name: extension.PredicateExpr, paramType: stringType, stateType: reflect.TypeOf(struct{ From string }{})}
	second := predicateKey{name: extension.PredicateExpr, paramType: stringType, stateType: reflect.TypeOf(struct{ To string }{})}
	if first == second {
		t.Fatal("predicate evaluator cache key collided across owning input types")
	}
}

func TestPredicateEvaluatorNopSkipsStateEvaluation(t *testing.T) {
	criteria, err := (&PredicateEvaluator{name: extension.PredicateNop}).Compute(context.Background(), "month")
	if err != nil {
		t.Fatal(err)
	}
	if criteria == nil || criteria.Expression != "" || len(criteria.Placeholders) != 0 {
		t.Fatalf("unexpected nop criteria: %#v", criteria)
	}
}

func TestValidatePredicateArgs_Duration(t *testing.T) {
	testCases := []struct {
		name      string
		predicate string
		value     interface{}
		args      []string
		wantErr   bool
	}{
		{
			name:      "thirty days requires seventh arg",
			predicate: "duration",
			value:     "thirty_days",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd"},
			wantErr:   true,
		},
		{
			name:      "thirty days with seventh arg",
			predicate: "duration",
			value:     "thirty_days",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd", "md"},
			wantErr:   false,
		},
		{
			name:      "week remains backward compatible with six args",
			predicate: "duration",
			value:     "week",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd"},
			wantErr:   false,
		},
	}
	for _, testCase := range testCases {
		err := validatePredicateArgs(testCase.predicate, testCase.value, testCase.args)
		if testCase.wantErr && err == nil {
			t.Fatalf("%s: expected error", testCase.name)
		}
		if !testCase.wantErr && err != nil {
			t.Fatalf("%s: unexpected error: %v", testCase.name, err)
		}
	}
}
