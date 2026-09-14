package velty

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

type predicateBinder struct {
	input any
	found bool
	err   error
}

func (b predicateBinder) Bind(context.Context, any) error { return nil }

func (b predicateBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key != xhandler.InputKey {
		return nil, false, nil
	}
	if b.err != nil {
		return nil, false, b.err
	}
	return b.input, b.found || b.input != nil, nil
}

type predicateContextKey struct{}

var observedPredicateContext any

type statusPredicate struct{}

func (p *statusPredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	observedPredicateContext = ctx.Value(predicateContextKey{})
	return &xpredicate.Criteria{
		Expression:   "u.status = ?",
		Placeholders: []any{value},
	}, nil
}

func TestPredicateRegistry_ContainsMigratedBuiltins(t *testing.T) {
	registry := predicateRegistry()
	expected := []string{
		predicateEqual,
		predicateNotEqual,
		predicateIn,
		predicateMultiIn,
		predicateNotIn,
		predicateMultiNotIn,
		predicateLessOrEqual,
		predicateLessThan,
		predicateGreaterOrEqual,
		predicateGreaterThan,
		predicateLike,
		predicateNotLike,
		predicateHandler,
		predicateContains,
		predicateNotContains,
		predicateIsNull,
		predicateIsNotNull,
		predicateExists,
		predicateNotExists,
		predicateLiteralIn,
		predicateExpr,
		predicateCriteriaExists,
		predicateCriteriaNotExists,
		predicateCriteriaIn,
		predicateCriteriaNotIn,
		predicateBetween,
		predicateDuration,
		predicateWhenPresent,
		predicateWhenNotPresent,
	}
	for _, name := range expected {
		if _, ok := registry[name]; !ok {
			t.Fatalf("expected builtin predicate %q to be registered", name)
		}
	}
}

func TestPredicateRegistry_AllBuiltinTemplatesCompileAndExecute(t *testing.T) {
	type rangePresence struct {
		ValueMin bool
		ValueMax bool
	}
	type rangeValue struct {
		ValueMin int
		ValueMax int
		Has      *rangePresence
	}
	testCases := []struct {
		name  string
		value any
		has   bool
		args  []string
	}{
		{name: predicateEqual, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateNotEqual, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateLessOrEqual, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateLessThan, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateGreaterOrEqual, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateGreaterThan, value: 7, has: true, args: []string{"u", "id"}},
		{name: predicateIn, value: []int{1, 2}, has: true, args: []string{"u", "id"}},
		{name: predicateMultiIn, value: []int{1, 2}, has: true, args: []string{"(u.a,u.b)"}},
		{name: predicateNotIn, value: []int{1, 2}, has: true, args: []string{"u", "id"}},
		{name: predicateMultiNotIn, value: []int{1, 2}, has: true, args: []string{"(u.a,u.b)"}},
		{name: predicateLike, value: "a%", has: true, args: []string{"u", "name"}},
		{name: predicateNotLike, value: "a%", has: true, args: []string{"u", "name"}},
		{name: predicateContains, value: "a", has: true, args: []string{"u", "name"}},
		{name: predicateNotContains, value: "a", has: true, args: []string{"u", "name"}},
		{name: predicateIsNull, value: true, has: true, args: []string{"u", "deleted_at"}},
		{name: predicateIsNotNull, value: true, has: true, args: []string{"u", "deleted_at"}},
		{name: predicateExists, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id"}},
		{name: predicateNotExists, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id"}},
		{name: predicateCriteriaExists, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id", "r.active = 1"}},
		{name: predicateCriteriaNotExists, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id", "r.active = 1"}},
		{name: predicateLiteralIn, value: []int{1, 2}, has: true, args: []string{"u.id"}},
		{name: predicateExpr, value: 7, has: true, args: []string{"u.id > ?"}},
		{name: predicateCriteriaIn, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id", "r.active = 1"}},
		{name: predicateCriteriaNotIn, value: []int{1}, has: true, args: []string{"u", "id", "r", "roles", "user_id", "role_id", "r.active = 1"}},
		{name: predicateBetween, value: rangeValue{ValueMin: 1, ValueMax: 9, Has: &rangePresence{ValueMin: true, ValueMax: true}}, has: true, args: []string{"u.id", "from", "to"}},
		{name: predicateDuration, value: "day", has: true, args: []string{"u.day", "CURRENT_DATE", "u.hour", "CURRENT_HOUR", "YESTERDAY", "WEEK_START", "MONTH_START"}},
		{name: predicateWhenPresent, value: "set", has: true, args: []string{"u.enabled = 1"}},
		{name: predicateWhenNotPresent, value: "", has: false, args: []string{"u.enabled = 0"}},
	}
	expectedSQL := map[string]string{
		predicateEqual:             "u.id = ?",
		predicateNotEqual:          "u.id != ?",
		predicateLessOrEqual:       "u.id <= ?",
		predicateLessThan:          "u.id < ?",
		predicateGreaterOrEqual:    "u.id >= ?",
		predicateGreaterThan:       "u.id > ?",
		predicateIn:                "u.id IN (?, ?)",
		predicateMultiIn:           "(u.a,u.b) IN (?, ?)",
		predicateNotIn:             "u.id NOT IN (?, ?)",
		predicateMultiNotIn:        "(u.a,u.b) NOT IN (?, ?)",
		predicateLike:              "u.name LIKE ?",
		predicateNotLike:           "u.name NOT LIKE ?",
		predicateContains:          "u.name LIKE ?",
		predicateNotContains:       "u.name NOT LIKE ?",
		predicateIsNull:            "u.deleted_at IS NULL",
		predicateIsNotNull:         "u.deleted_at IS NOT NULL",
		predicateExists:            "EXISTS (SELECT 1 FROM roles r WHERE r.user_id = u.id AND r.role_id IN (?))",
		predicateNotExists:         "NOT EXISTS (SELECT 1 FROM roles r WHERE r.user_id = u.id AND r.role_id IN (?))",
		predicateCriteriaExists:    "EXISTS (SELECT 1 FROM roles r WHERE r.user_id = u.id AND r.active = 1 AND r.role_id IN (?))",
		predicateCriteriaNotExists: "NOT EXISTS (SELECT 1 FROM roles r WHERE r.user_id = u.id AND r.active = 1 AND r.role_id IN (?))",
		predicateLiteralIn:         "u.id IN (?, ?)",
		predicateExpr:              "u.id > ?",
		predicateCriteriaIn:        "u.id IN (SELECT r.user_id FROM roles r WHERE r.active = 1 AND r.role_id IN (?))",
		predicateCriteriaNotIn:     "u.id NOT IN (SELECT r.user_id FROM roles r WHERE r.active = 1 AND r.role_id IN (?))",
		predicateBetween:           "u.id BETWEEN ? AND ?",
		predicateDuration:          "u.day = CURRENT_DATE",
		predicateWhenPresent:       "u.enabled = 1",
		predicateWhenNotPresent:    "u.enabled = 0",
	}
	expectedArgs := map[string][]any{
		predicateEqual:             {7},
		predicateNotEqual:          {7},
		predicateLessOrEqual:       {7},
		predicateLessThan:          {7},
		predicateGreaterOrEqual:    {7},
		predicateGreaterThan:       {7},
		predicateIn:                {1, 2},
		predicateMultiIn:           {1, 2},
		predicateNotIn:             {1, 2},
		predicateMultiNotIn:        {1, 2},
		predicateLike:              {"a%"},
		predicateNotLike:           {"a%"},
		predicateContains:          {"%a%"},
		predicateNotContains:       {"%a%"},
		predicateExists:            {1},
		predicateNotExists:         {1},
		predicateCriteriaExists:    {1},
		predicateCriteriaNotExists: {1},
		predicateLiteralIn:         {1, 2},
		predicateExpr:              {7},
		predicateCriteriaIn:        {1},
		predicateCriteriaNotIn:     {1},
		predicateBetween:           {1, 9},
	}
	registry := predicateRegistry()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			entry, ok := registry[testCase.name]
			if !ok || entry.template == nil {
				t.Fatalf("missing builtin template")
			}
			evaluator, err := newTemplateEvaluator(entry.template, reflect.TypeOf(testCase.value), testCase.args)
			if err != nil {
				t.Fatalf("compile failed: %v", err)
			}
			fragment, args, err := evaluator.evaluate(context.Background(), testCase.value, testCase.has)
			if err != nil {
				t.Fatalf("evaluate failed: %v", err)
			}
			actualSQL := strings.Join(strings.Fields(fragment), " ")
			if actualSQL != expectedSQL[testCase.name] {
				t.Fatalf("unexpected SQL: got %q want %q", actualSQL, expectedSQL[testCase.name])
			}
			if !reflect.DeepEqual(args, expectedArgs[testCase.name]) {
				t.Fatalf("unexpected args: got %#v want %#v", args, expectedArgs[testCase.name])
			}
		})
	}
}

func TestProgram_ContextExpandsBuiltinAndHandlerPredicates(t *testing.T) {
	component := &spec.Component{
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: `SELECT * FROM users u ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`,
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "IDs", Predicates: []*spec.Predicate{{Group: 0, Name: predicateIn, Args: []string{"u", "id"}}}},
			{Name: "Search", Predicates: []*spec.Predicate{{Group: 0, Name: predicateContains, Args: []string{"u", "name"}}}},
			{Name: "Status", Predicates: []*spec.Predicate{{Group: 0, Name: predicateHandler, Args: []string{"statusPredicate"}}}},
		},
	}
	type input struct {
		IDs    []int
		Search string
		Status string
	}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeOf(input{}), Lookup: func(name string) (reflect.Type, error) {
		if name == "statusPredicate" {
			return reflect.TypeOf(statusPredicate{}), nil
		}
		return nil, nil
	}})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	actualInput := input{
		IDs:    []int{7, 8},
		Search: "jo",
		Status: "active",
	}
	var args []any
	actual, err := program.NewContext(context.WithValue(context.Background(), predicateContextKey{}, "caller"), predicateBinder{input: &actualInput}, func(values ...any) {
		args = append(args, values...)
	})
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}
	ctx := actual.(*Context)
	fragment, err := ctx.FilterGroup(0, "AND")
	if err != nil {
		t.Fatalf("FilterGroup failed: %v", err)
	}
	if !strings.Contains(fragment, "u.id IN (?, ?)") {
		t.Fatalf("expected IN predicate SQL, got %s", fragment)
	}
	if !strings.Contains(fragment, "u.name LIKE ?") {
		t.Fatalf("expected contains predicate SQL, got %s", fragment)
	}
	if !strings.Contains(fragment, "u.status = ?") {
		t.Fatalf("expected handler predicate SQL, got %s", fragment)
	}
	expectedArgs := []any{7, 8, "%jo%", "active"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected predicate args: %#v", args)
	}
	if observedPredicateContext != "caller" {
		t.Fatalf("custom predicate did not receive caller context: %#v", observedPredicateContext)
	}
}

func TestProgram_ContextUsesPresenceMarkerInsteadOfZeroHeuristic(t *testing.T) {
	type has struct {
		Limit bool
	}
	type input struct {
		Limit int
		Has   *has `setMarker:"true"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name:       "Limit",
		Predicates: []*spec.Predicate{{Name: predicateEqual, Args: []string{"u", "limit"}}},
	}}}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	for _, testCase := range []struct {
		name     string
		input    input
		wantSQL  bool
		wantArgs []any
	}{
		{name: "absent nonzero", input: input{Limit: 9}, wantSQL: false},
		{name: "present zero", input: input{Limit: 0, Has: &has{Limit: true}}, wantSQL: true, wantArgs: []any{0}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var args []any
			actual, err := program.NewContext(context.Background(), predicateBinder{input: &testCase.input}, func(values ...any) {
				args = append(args, values...)
			})
			if err != nil {
				t.Fatalf("NewContext failed: %v", err)
			}
			fragment, err := actual.(*Context).FilterGroup(0, "AND")
			if err != nil {
				t.Fatalf("expand failed: %v", err)
			}
			if (fragment != "") != testCase.wantSQL || !reflect.DeepEqual(args, testCase.wantArgs) {
				t.Fatalf("unexpected presence result: fragment=%q args=%#v", fragment, args)
			}
		})
	}
}

func TestProgram_ApplyWhenAbsentEvaluatesPredicate(t *testing.T) {
	type has struct {
		Limit bool
	}
	type input struct {
		Limit int
		Has   *has `setMarker:"true"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name: "Limit",
		Predicates: []*spec.Predicate{{
			Name: predicateEqual, Args: []string{"u", "limit"}, ApplyWhenAbsent: true,
		}},
	}}}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	var args []any
	actual, err := program.NewContext(context.Background(), predicateBinder{input: &input{}}, func(values ...any) {
		args = append(args, values...)
	})
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}
	fragment, err := actual.(*Context).FilterGroup(0, "AND")
	if err != nil {
		t.Fatalf("expand failed: %v", err)
	}
	if !strings.Contains(fragment, "u.limit = ?") || !reflect.DeepEqual(args, []any{0}) {
		t.Fatalf("absent predicate was not applied: fragment=%q args=%#v", fragment, args)
	}
}

func TestProgram_NewContextRequiresScopedInput(t *testing.T) {
	program := &Program{}
	lookupErr := errors.New("lookup failed")
	testCases := []struct {
		name   string
		binder xhandler.Binder
		want   string
	}{
		{name: "nil binder", want: "predicate input binder is required"},
		{name: "missing input", binder: predicateBinder{}, want: "predicate input is not available"},
		{name: "lookup failure", binder: predicateBinder{err: lookupErr}, want: "resolve predicate input: lookup failed"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := program.NewContext(context.Background(), testCase.binder, nil)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("expected %q error, got %v", testCase.want, err)
			}
		})
	}
}

func TestCompile_UsesCanonicalShapeFieldLookup(t *testing.T) {
	type input struct {
		FirstName string
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name:       "first_name",
		Predicates: []*spec.Predicate{{Name: predicateEqual, Args: []string{"v", "first_name"}}},
	}}}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(program.predicates) != 1 || !reflect.DeepEqual(program.predicates[0].fieldIndex, []int{0}) {
		t.Fatalf("unexpected compiled predicate: %#v", program.predicates)
	}
}

func TestCompileUsesCompiledBindingPathForLogicalAlias(t *testing.T) {
	type input struct {
		Projection []string
	}
	param := &spec.Parameter{
		Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"},
		Predicates: []*spec.Predicate{{Name: predicateIn, Args: []string{"u", "field"}}},
	}
	program, err := Compile(CompileInput{
		Component: &spec.Component{Parameters: []*spec.Parameter{param}},
		InputType: reflect.TypeOf(input{}),
		Bindings:  []bindly.BindingSpec{{Path: "Projection", Name: "Fields", Extension: param}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(program.predicates) != 1 || program.predicates[0].fieldName != "Projection" ||
		!reflect.DeepEqual(program.predicates[0].fieldIndex, []int{0}) {
		t.Fatalf("predicates = %+v", program.predicates)
	}
}
