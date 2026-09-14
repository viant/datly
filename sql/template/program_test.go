package template

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	sqlmacro "github.com/viant/datly/sql/macro"
	xhandler "github.com/viant/xdatly/handler"
)

type templateBinder map[xhandler.ValueKey]any

func (b templateBinder) Bind(context.Context, any) error { return nil }

func (b templateBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	value, ok := b[key]
	return value, ok, nil
}

type loopDriverValue int64

func (v *loopDriverValue) Value() (driver.Value, error) {
	return int64(*v), nil
}

func TestCompiler_DefinesPredicateUsedInControlExpression(t *testing.T) {
	type input struct{}
	predicateProgram, err := readerpredicate.Compile(readerpredicate.CompileInput{})
	if err != nil {
		t.Fatalf("predicate compile failed: %v", err)
	}
	program, err := (Compiler{
		Source:    `#if($predicate.FilterGroup(99, "AND") == "") SELECT 1 #end`,
		InputType: reflect.TypeOf(input{}),
		Predicate: predicateProgram,
	}).Compile()
	if err != nil {
		t.Fatalf("SQL template compile failed: %v", err)
	}
	actualInput := input{}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(actualInput), Binder: templateBinder{xhandler.InputKey: &actualInput}})
	if err != nil || !strings.Contains(result.SQL, "SELECT 1") {
		t.Fatalf("unexpected predicate control result: SQL=%q err=%v", result.SQL, err)
	}
}

func TestCompiler_CompilesPredicateOnlySQL(t *testing.T) {
	type input struct{}
	predicateProgram, err := readerpredicate.Compile(readerpredicate.CompileInput{})
	if err != nil {
		t.Fatalf("predicate compile failed: %v", err)
	}
	program, err := (Compiler{
		Source:    `SELECT 1 ${predicate.Builder().CombineAnd($predicate.FilterGroup(99, "AND")).Build("WHERE")}`,
		InputType: reflect.TypeOf(input{}),
		Predicate: predicateProgram,
	}).Compile()
	if err != nil {
		t.Fatalf("SQL template compile failed: %v", err)
	}
	if program == nil {
		t.Fatal("predicate-only SQL must compile")
	}
	actualInput := input{}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(actualInput), Binder: templateBinder{xhandler.InputKey: &actualInput}})
	if err != nil || strings.TrimSpace(result.SQL) != "SELECT 1" {
		t.Fatalf("unexpected predicate-only result: SQL=%q err=%v", result.SQL, err)
	}
}

func TestCompiler_ResolvesTypedBinderVariablePerEvaluation(t *testing.T) {
	type input struct{}
	program, err := (Compiler{
		Source:    `SELECT id FROM users WHERE tenant_id = $Tenant`,
		InputType: reflect.TypeOf(input{}),
		Variables: []Variable{{Name: "Tenant", Type: reflect.TypeOf(int(0)), Key: xhandler.ValueKey("Tenant"), Required: true}},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	for _, testCase := range []struct {
		name   string
		binder templateBinder
		want   int
	}{
		{name: "first scope", binder: templateBinder{"Tenant": 3}, want: 3},
		{name: "second scope", binder: templateBinder{"Tenant": int32(7)}, want: 7},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{}), Binder: testCase.binder})
			if err != nil {
				t.Fatalf("evaluate failed: %v", err)
			}
			if result.SQL != `SELECT id FROM users WHERE tenant_id = ?` || !reflect.DeepEqual(result.Args, []any{testCase.want}) {
				t.Fatalf("unexpected binder result: SQL=%q args=%#v", result.SQL, result.Args)
			}
		})
	}
	if _, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{}), Binder: templateBinder{}}); err == nil || !strings.Contains(err.Error(), "is not available") {
		t.Fatalf("expected missing required value error, got %v", err)
	}
	if _, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{}), Binder: templateBinder{"Tenant": "wrong"}}); err == nil || !strings.Contains(err.Error(), "not int") {
		t.Fatalf("expected binder type error, got %v", err)
	}
}

func TestCompiler_CompileAndEvaluateTypedInput(t *testing.T) {
	type input struct {
		VendorID int
	}
	program, err := (Compiler{
		Source: `SELECT id FROM vendor WHERE 1=1
#if($vendorID < 0)
AND 1=2
#end`,
		InputType: reflect.TypeOf(input{}),
		Variables: []Variable{{Name: "vendorID", FieldIndex: []int{0}}},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program == nil {
		t.Fatal("expected compiled program")
	}
	for _, testCase := range []struct {
		name      string
		input     input
		wantGuard bool
	}{
		{name: "condition false", input: input{VendorID: 1}},
		{name: "condition true", input: input{VendorID: -1}, wantGuard: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(testCase.input)})
			if err != nil {
				t.Fatalf("evaluate failed: %v", err)
			}
			if actual := strings.Contains(result.SQL, "AND 1=2"); actual != testCase.wantGuard {
				t.Fatalf("guard presence: got %v, SQL: %s", actual, result.SQL)
			}
		})
	}
}

func TestCompiler_CompileSupportsUnsafeInput(t *testing.T) {
	type input struct {
		Enabled bool
	}
	program, err := (Compiler{
		Source:    `SELECT 1 #if($Unsafe.Enabled) WHERE 1=1 #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(&input{Enabled: true})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "WHERE 1=1") {
		t.Fatalf("expected $Unsafe condition output, got %s", result.SQL)
	}
}

func TestCompiler_CompileSupportsDirectUnsafeEmission(t *testing.T) {
	type input struct {
		Table string
	}
	program, err := (Compiler{
		Source:    `SELECT id FROM $Unsafe.Table`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program == nil {
		t.Fatal("direct $Unsafe emission must compile")
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Table: "vendors"})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if result.SQL != "SELECT id FROM vendors" || len(result.Args) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestCompiler_CompileProtectsQuotedUnsafeWhileEmittingCodeUnsafe(t *testing.T) {
	type input struct{ Table string }
	program, err := (Compiler{
		Source:    `SELECT '$Unsafe.Table' AS literal FROM $Unsafe.Table`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Table: "vendors"})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if result.SQL != `SELECT '$Unsafe.Table' AS literal FROM vendors` {
		t.Fatalf("SQL = %q", result.SQL)
	}
}

func TestCompiler_CompileSupportsInvocationViewContext(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source: `#if($Enabled && $View.Limit > 0)
SELECT id FROM product WHERE 1=1 $View.ParentJoinOn("AND", "vendor_id")
#end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{Enabled: true}),
		View:  ViewInput{ParentValues: []any{7, 9}, Limit: 10},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "AND vendor_id IN (?, ?)") {
		t.Fatalf("unexpected parent-key SQL: %q", result.SQL)
	}
	if !reflect.DeepEqual(result.Args, []any{7, 9}) || !result.ParentBindings {
		t.Fatalf("unexpected parent-key result: args=%#v parent=%v", result.Args, result.ParentBindings)
	}

	excluded, err := program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{Enabled: true}),
		View:  ViewInput{ParentValues: []any{7, 9}, Limit: 10, ExcludeParent: true},
	})
	if err != nil {
		t.Fatalf("excluded evaluation failed: %v", err)
	}
	if strings.Contains(excluded.SQL, "vendor_id IN") || len(excluded.Args) != 0 || !excluded.ParentBindings {
		t.Fatalf("unexpected excluded parent-key result: SQL=%q args=%#v parent=%v", excluded.SQL, excluded.Args, excluded.ParentBindings)
	}
}

func TestHasTemplateCodeDetectsViewProperties(t *testing.T) {
	source, _ := sqlmacro.PrepareNonWindowSQLTemplate(`SELECT $View.Limit FROM ($View.Users.NonWindowSQL) parent`, "Users")
	if !hasTemplateCode(source, viewVariable) {
		t.Fatalf("expected $View template code in %q", source)
	}
}

func TestCompiler_PreservesProtectedViewProperties(t *testing.T) {
	type input struct{}
	program, err := (Compiler{
		Source: `SELECT '$View.Limit' AS literal_value, $View.Limit AS limit_value
-- $View.Offset
FROM ($View.Users.NonWindowSQL) parent`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{}),
		View: ViewInput{
			Limit: 7, Offset: 14,
			NonWindowSQL: "SELECT id FROM users",
		},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, `'$View.Limit'`) || !strings.Contains(result.SQL, `-- $View.Offset`) {
		t.Fatalf("protected $View text changed: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, `7 AS limit_value`) {
		t.Fatalf("executable $View property was not evaluated: %s", result.SQL)
	}
}

func TestCompiler_ViewContextRejectsStructuralColumnInjection(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source:    `#if($Enabled) SELECT 1 $View.ParentJoinOn("AND", "id OR 1=1") #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	_, err = program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{Enabled: true}),
		View:  ViewInput{ParentValues: []any{7}},
	})
	if err == nil || !strings.Contains(err.Error(), "not a column reference") {
		t.Fatalf("expected structural column validation error, got %v", err)
	}
}

func TestCompiler_CompileSupportsCompositeInvocationViewContext(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source:    `#if($Enabled) SELECT id FROM signal WHERE 1=1 $View.ParentCompositeJoinOn("AND", "kind", "value") #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{Enabled: true}),
		View: ViewInput{ParentCompositeValues: [][]interface{}{
			{"country", "PL"},
			{"country", "US"},
		}},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "AND (kind, value) IN ((?, ?), (?, ?))") {
		t.Fatalf("unexpected composite SQL: %q", result.SQL)
	}
	if !reflect.DeepEqual(result.Args, []any{"country", "PL", "country", "US"}) {
		t.Fatalf("unexpected composite args: %#v", result.Args)
	}
}

func TestCompiler_CompileSupportsNamedParentNonWindowSQL(t *testing.T) {
	type input struct {
		Enabled bool
		Prefix  int
		Suffix  int
	}
	program, err := (Compiler{
		Source: `#if($Enabled) SELECT $Prefix AS prefix_value, COUNT(*) AS total
FROM ($View.Users.NonWindowSQL) parent
WHERE parent.id <= $Suffix
OR parent.id IN (SELECT id FROM ($View.NonWindowSQL) repeated) #end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{
		Input: reflect.ValueOf(input{Enabled: true, Prefix: 3, Suffix: 9}),
		View: ViewInput{
			NonWindowSQL:  "SELECT id FROM users WHERE tenant_id = ? AND active = ?",
			NonWindowArgs: []any{7, true},
		},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if strings.Count(result.SQL, "SELECT id FROM users WHERE tenant_id = ? AND active = ?") != 2 {
		t.Fatalf("unexpected parent SQL expansion: %s", result.SQL)
	}
	if !reflect.DeepEqual(result.Args, []any{7, true, 7, true}) {
		t.Fatalf("unexpected repeated parent args: %#v", result.Args)
	}

	excluded, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{})})
	if err != nil {
		t.Fatalf("inactive branch must not require parent SQL: %v", err)
	}
	if strings.TrimSpace(excluded.SQL) != "" || len(excluded.Args) != 0 {
		t.Fatalf("unexpected inactive result: SQL=%q args=%#v", excluded.SQL, excluded.Args)
	}
}

func TestCompiler_NonWindowSQLRequiresParentQueryWhenEvaluated(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source:           `#if($Enabled) SELECT COUNT(*) FROM ($View.Users.NonWindowSQL) parent #end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	_, err = program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Enabled: true})})
	if err == nil || !strings.Contains(err.Error(), "requires a parent non-window query") {
		t.Fatalf("expected missing parent query error, got %v", err)
	}
}

func TestCompiler_RejectsUnknownNamedParentNonWindowSQL(t *testing.T) {
	type input struct{ Enabled bool }
	_, err := (Compiler{
		Source:           `#if($Enabled) SELECT COUNT(*) FROM ($View.Other.NonWindowSQL) parent #end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err == nil {
		t.Fatal("expected unknown named parent access to fail compilation")
	}
}

func TestCompiler_PreservesProtectedNamedNonWindowSQLText(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source: `#if($Enabled)
SELECT '$View.Users.NonWindowSQL' AS literal_value
-- $View.Users.NonWindowSQL
#end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Enabled: true})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, `'$View.Users.NonWindowSQL'`) || !strings.Contains(result.SQL, `-- $View.Users.NonWindowSQL`) {
		t.Fatalf("protected SQL text changed: %s", result.SQL)
	}
}

func TestCompiler_PreservesProtectedUnknownNonWindowSQLText(t *testing.T) {
	type input struct{ Enabled bool }
	program, err := (Compiler{
		Source: `#if($Enabled)
SELECT '$View.Other.NonWindowSQL' AS literal_value
-- $View.Unknown.NonWindowSQL
#end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Enabled: true})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, `'$View.Other.NonWindowSQL'`) || !strings.Contains(result.SQL, `-- $View.Unknown.NonWindowSQL`) {
		t.Fatalf("protected unknown SQL text changed: %s", result.SQL)
	}
}

func TestCompiler_CompileSkipsPlainSQLAndRejectsInvalidTemplate(t *testing.T) {
	type input struct{ ID int }
	plainSources := []string{
		"SELECT id FROM users WHERE id = :ID",
		"SELECT '#if($ID > 0)' AS literal",
		"SELECT 1 -- #if($ID > 0)\n",
		"SELECT 1 /* #if($ID > 0) */",
		"SELECT $$#if($ID > 0)$$ AS literal",
		"SELECT '${predicate.FilterGroup(0, \"AND\")}' AS literal",
		"SELECT 1 -- ${predicate.FilterGroup(0, \"AND\")}\n",
	}
	for _, source := range plainSources {
		plain, err := (Compiler{Source: source, InputType: reflect.TypeOf(input{})}).Compile()
		if err != nil {
			t.Fatalf("plain SQL compile failed for %q: %v", source, err)
		}
		if plain != nil {
			t.Fatalf("plain SQL must not allocate a template evaluator for %q", source)
		}
	}
	if _, err := (Compiler{Source: "#if($ID > 0) SELECT 1", InputType: reflect.TypeOf(input{})}).Compile(); err == nil {
		t.Fatal("expected unterminated template to fail")
	}
}

func TestCompiler_CompileBindsEmittedInputAlias(t *testing.T) {
	type input struct{ VendorID int }
	program, err := (Compiler{
		Source:    "#if($vendorID > 0) SELECT id FROM vendor WHERE id = $vendorID #end",
		InputType: reflect.TypeOf(input{}),
		Variables: []Variable{{Name: "vendorID", FieldIndex: []int{0}}},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{VendorID: 7})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "id = :VendorID") {
		t.Fatalf("expected emitted alias to become a named placeholder, got %q", result.SQL)
	}
	if strings.Contains(result.SQL, "id = 7") {
		t.Fatalf("input alias was interpolated into SQL: %q", result.SQL)
	}
}

func TestCompiler_CompileBindsNestedEmittedInputAlias(t *testing.T) {
	type filter struct{ ID int }
	type input struct{ Filter filter }
	program, err := (Compiler{
		Source:    "#if($Filter.ID > 0) SELECT $Filter.ID #end",
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Filter: filter{ID: 7}})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "SELECT ?") || !reflect.DeepEqual(result.Args, []any{7}) {
		t.Fatalf("unexpected nested binding result: SQL=%q args=%#v", result.SQL, result.Args)
	}
}

func TestCompiler_CompileBindsSetLocal(t *testing.T) {
	type input struct {
		Name string
	}
	program, err := (Compiler{
		Source: `#set($name = $Name)
SELECT $name;`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Name: "acme"})})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if strings.Count(result.SQL, "SELECT ?") != 1 {
		t.Fatalf("expected local value to use a placeholder, got %q", result.SQL)
	}
	if !reflect.DeepEqual(result.Args, []any{"acme"}) {
		t.Fatalf("unexpected local binding args: %#v", result.Args)
	}
}

func TestCompiler_CompileBindsEmittedLoopLocals(t *testing.T) {
	type identifier int
	type item struct {
		ID   int
		Name string
	}
	type input struct {
		IDs    []int
		Names  []string
		Items  []item
		Refs   []*item
		Typed  []identifier
		Bytes  [][]byte
		Times  []time.Time
		Values []*loopDriverValue
	}
	testCases := []struct {
		name string
		sql  string
		want []any
	}{
		{name: "scalar", sql: `#foreach($id in $IDs) SELECT $id; #end`, want: []any{1, 2, 3}},
		{name: "string", sql: `#foreach($name in $Names) SELECT $name; #end`, want: []any{"a", "b"}},
		{name: "struct field", sql: `#foreach($item in $Items) SELECT $item.ID, $item.Name; #end`, want: []any{4, "four", 5, "five"}},
		{name: "pointer field", sql: `#foreach($item in $Refs) SELECT $item.ID; #end`, want: []any{6, 7}},
		{name: "defined scalar", sql: `#foreach($id in $Typed) SELECT $id; #end`, want: []any{identifier(8), identifier(9)}},
		{name: "bytes", sql: `#foreach($value in $Bytes) SELECT $value; #end`, want: []any{[]byte("x"), []byte("y")}},
		{name: "time", sql: `#foreach($value in $Times) SELECT $value; #end`, want: []any{time.Unix(1, 0), time.Unix(2, 0)}},
	}
	firstValue := loopDriverValue(11)
	secondValue := loopDriverValue(12)
	actualInput := input{
		IDs:    []int{1, 2, 3},
		Names:  []string{"a", "b"},
		Items:  []item{{ID: 4, Name: "four"}, {ID: 5, Name: "five"}},
		Refs:   []*item{{ID: 6}, {ID: 7}},
		Typed:  []identifier{8, 9},
		Bytes:  [][]byte{[]byte("x"), []byte("y")},
		Times:  []time.Time{time.Unix(1, 0), time.Unix(2, 0)},
		Values: []*loopDriverValue{&firstValue, &secondValue},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			program, err := (Compiler{Source: testCase.sql, InputType: reflect.TypeOf(input{})}).Compile()
			if err != nil {
				t.Fatalf("compile failed: %v", err)
			}
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(actualInput)})
			if err != nil {
				t.Fatalf("evaluate failed: %v", err)
			}
			if strings.Count(result.SQL, "?") != len(testCase.want) || !reflect.DeepEqual(result.Args, testCase.want) {
				t.Fatalf("unexpected loop binding: SQL=%q args=%#v want=%#v", result.SQL, result.Args, testCase.want)
			}
		})
	}
	t.Run("pointer driver valuer", func(t *testing.T) {
		program, err := (Compiler{Source: `#foreach($value in $Values) SELECT $value; #end`, InputType: reflect.TypeOf(input{})}).Compile()
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(actualInput)})
		if err != nil {
			t.Fatalf("evaluate failed: %v", err)
		}
		if len(result.Args) != 2 {
			t.Fatalf("unexpected valuer arguments: %#v", result.Args)
		}
		for i, want := range []int64{11, 12} {
			valuer, ok := result.Args[i].(driver.Valuer)
			if !ok {
				t.Fatalf("argument %d lost driver.Valuer: %T", i, result.Args[i])
			}
			actual, err := valuer.Value()
			if err != nil || actual != want {
				t.Fatalf("unexpected driver value %d: value=%v err=%v", i, actual, err)
			}
		}
	})
}

func TestCompiler_CompileRejectsBoundValueInsideSQLQuote(t *testing.T) {
	type input struct{ Name string }
	_, err := (Compiler{
		Source:    `#set($name = $Name) #if(true) SELECT '$name' #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err == nil || !strings.Contains(err.Error(), "inside SQL quoted text") {
		t.Fatalf("expected quoted bound-value error, got %v", err)
	}
}

func TestCompiler_CompileValidatesFieldIndex(t *testing.T) {
	type input struct{ ID int }
	_, err := (Compiler{
		Source:    "#if($Alias > 0) SELECT 1 #end",
		InputType: reflect.TypeOf(input{}),
		Variables: []Variable{{Name: "Alias", FieldIndex: []int{4}}},
	}).Compile()
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("expected invalid field-index error, got %v", err)
	}
}

func TestCompiler_CompileUsesTypesForUnusedInterfaceAndStructSliceVariables(t *testing.T) {
	type row struct{ ID *int64 }
	type input struct {
		Dynamic any
		Rows    []*row
	}
	program, err := (Compiler{
		Source:    `#foreach($row in $Rows) SELECT $row.ID; #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil || program == nil {
		t.Fatalf("compile typed variables: program=%v error=%v", program, err)
	}
}

func TestCompiled_EvaluateRejectsNilEmbeddedFieldPath(t *testing.T) {
	type Embedded struct{ Enabled bool }
	type input struct{ *Embedded }
	program, err := (Compiler{
		Source:    "#if($enabled) SELECT 1 #end",
		InputType: reflect.TypeOf(input{}),
		Variables: []Variable{{Name: "enabled", FieldIndex: []int{0, 0}}},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	_, err = program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{})})
	if err == nil || !strings.Contains(err.Error(), "read SQL template variable $enabled") {
		t.Fatalf("expected nil embedded-field error, got %v", err)
	}
}

func TestCompiled_EvaluateRejectsWrongInputType(t *testing.T) {
	type input struct{ ID int }
	program, err := (Compiler{Source: "#if($ID > 0) SELECT 1 #end", InputType: reflect.TypeOf(input{})}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if _, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(struct{ ID int64 }{ID: 1})}); err == nil {
		t.Fatal("expected wrong input type to fail")
	}
}
