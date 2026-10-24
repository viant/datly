package velty

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xpredicate "github.com/viant/xdatly/predicate"
)

// Options are explicitly allowlisted by the handler; bound tenant is never a
// configurable attribute. Real handlers can retain their own existing defaults.
type configuredPredicate struct {
	tenant int
	calls  int
}

func (p *configuredPredicate) Compute(ctx context.Context, value any, args ...any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.calls++
	if p.calls > 1 {
		return nil, fmt.Errorf("instance reused")
	}
	alias, enabled, offset := "r", true, 0
	pairs, err := xpredicate.Pairs(args...)
	if err != nil {
		return nil, err
	}
	for name, value := range pairs {
		switch name {
		case "alias":
			var ok bool
			alias, ok = value.(string)
			if !ok || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(alias) {
				return nil, fmt.Errorf("invalid alias")
			}
		case "enabled":
			var ok bool
			enabled, ok = value.(bool)
			if !ok {
				return nil, fmt.Errorf("enabled requires bool")
			}
		case "offset":
			var ok bool
			offset, ok = value.(int)
			if !ok {
				return nil, fmt.Errorf("offset requires int")
			}
		default:
			return nil, fmt.Errorf("unknown option %q", name)
		}
	}
	// Prove changing the variadic slice does not mutate compiled arguments.
	if len(args) > 0 {
		args[0] = "changed"
	}
	if !enabled {
		return nil, fmt.Errorf("denied")
	}
	return &xpredicate.Criteria{Expression: alias + ".tenant_id = ? AND " + alias + ".id >= ?", Placeholders: []any{p.tenant, value.(int) + offset}}, nil
}

type configuredBinder struct {
	predicateBinder
	tenant int
	fail   bool
}

func (b configuredBinder) Bind(_ context.Context, target any) error {
	if b.fail {
		return fmt.Errorf("binding failed")
	}
	target.(*configuredPredicate).tenant = b.tenant
	return nil
}
func configuredProgram(args []any) (*Program, error) {
	type input struct {
		Minimum int
		Maximum int
	}
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Minimum", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"Configured"}, HandlerArgs: args}}},
		{Name: "Maximum", Predicates: []*spec.Predicate{{Name: "less_or_equal", Args: []string{"r", "id"}}}},
	}}
	return Compile(CompileInput{Component: component, InputType: reflect.TypeFor[input](), Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[configuredPredicate](), nil }})
}
func TestVariadicHandlerSQLiteAndIsolation(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES(1,7),(2,7),(3,8)"); err != nil {
		t.Fatal(err)
	}
	type input struct {
		Minimum int
		Maximum int
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	for _, args := range [][]any{nil, {"alias", "r", "enabled", true, "offset", 0}} {
		program, err := configuredProgram(args)
		if err != nil {
			t.Fatal(err)
		}
		for _, tenant := range []int{7, 8} {
			var bound []any
			ctx, err := program.NewContext(context.Background(), configuredBinder{predicateBinder: predicateBinder{input: &input{1, 3}}, tenant: tenant}, func(args ...any) { bound = append(bound, args...) })
			if err != nil {
				t.Fatal(err)
			}
			fragment, err := ctx.(*Context).Expand(0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(bound, []any{tenant, 1, 3}) {
				t.Fatalf("ordered args %#v", bound)
			}
			expected := []row{{1}, {2}}
			if tenant == 8 {
				expected = []row{{3}}
			}
			h.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT r.id FROM records r WHERE " + fragment + " ORDER BY r.id", Args: bound}, expected)
		}
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(tenant int) {
				defer wg.Done()
				criteria, err := program.Criteria(context.Background(), configuredBinder{predicateBinder: predicateBinder{input: &input{1, 3}}, tenant: tenant}, 0)
				if err != nil {
					t.Error(err)
					return
				}
				if criteria.Placeholders[0] != tenant {
					t.Errorf("cross-invocation tenant %#v", criteria)
				}
			}(i)
		}
		wg.Wait()
	}
}
func TestVariadicHandlerErrors(t *testing.T) {
	type input struct {
		Minimum int
		Maximum int
	}
	for _, args := range [][]any{{"odd"}, {"tenant", 7}, {"alias", "r; DROP"}, {"enabled", "true"}, {"offset", true}, {"enabled", false}} {
		program, err := configuredProgram(args)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = program.Criteria(context.Background(), configuredBinder{predicateBinder: predicateBinder{input: &input{1, 3}}, tenant: 7}, 0); err == nil {
			t.Fatalf("accepted %#v", args)
		}
	}
	program, err := configuredProgram(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = program.Criteria(ctx, configuredBinder{predicateBinder: predicateBinder{input: &input{1, 3}}}, 0); err != context.Canceled {
		t.Fatalf("cancel %v", err)
	}
	if _, err = program.Criteria(context.Background(), configuredBinder{predicateBinder: predicateBinder{input: &input{1, 3}}, fail: true}, 0); err == nil || !strings.Contains(err.Error(), "binding failed") {
		t.Fatalf("bind %v", err)
	}
	if _, err = configuredProgram([]any{map[string]any{"alias": "r"}}); err == nil {
		t.Fatal("mutable metadata accepted")
	}
}

func TestHandlerArgumentCompilationCompatibility(t *testing.T) {
	type input struct{ Minimum int }
	for _, tc := range []struct {
		name   string
		typ    reflect.Type
		legacy []string
		typed  []any
		fail   bool
	}{
		{name: "old unchanged", typ: reflect.TypeFor[tenantPredicate]()},
		{name: "old typed extras rejected", typ: reflect.TypeFor[tenantPredicate](), typed: []any{"alias", "r"}, fail: true},
		{name: "old string extras rejected", typ: reflect.TypeFor[tenantPredicate](), legacy: []string{"alias", "r"}, fail: true},
		{name: "new string metadata", typ: reflect.TypeFor[configuredPredicate](), legacy: []string{"alias", "r"}},
		{name: "new typed metadata", typ: reflect.TypeFor[configuredPredicate](), typed: []any{"enabled", true, "offset", 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := &spec.Predicate{Name: "handler", Args: append([]string{"test.Handler"}, tc.legacy...), HandlerArgs: tc.typed}
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Minimum", Predicates: []*spec.Predicate{definition}}}}
			program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeFor[input](), Lookup: func(string) (reflect.Type, error) { return tc.typ, nil }})
			if tc.fail {
				if err == nil {
					t.Fatal("accepted incompatible handler")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.typ == reflect.TypeFor[configuredPredicate]() {
				if _, err = program.Criteria(context.Background(), configuredBinder{predicateBinder: predicateBinder{input: &input{1}}, tenant: 7}, 0); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type positionalPredicate struct{}

func (*positionalPredicate) Compute(_ context.Context, value any, args ...any) (*xpredicate.Criteria, error) {
	return &xpredicate.Criteria{Expression: "custom = ?", Placeholders: append([]any{value}, args...)}, nil
}
func TestVariadicPositionalValues(t *testing.T) {
	type input struct{ Minimum int }
	for _, args := range [][]any{nil, {"text", true, 7, nil}} {
		definition := &spec.Predicate{Name: "handler", Args: []string{"test.Positional"}, HandlerArgs: args}
		program, err := Compile(CompileInput{Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Minimum", Predicates: []*spec.Predicate{definition}}}}, InputType: reflect.TypeFor[input](), Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[positionalPredicate](), nil }})
		if err != nil {
			t.Fatal(err)
		}
		got, err := program.Criteria(context.Background(), predicateBinder{input: &input{2}}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Placeholders, append([]any{2}, args...)) {
			t.Fatalf("lost positional types: %#v", got)
		}
	}
}
