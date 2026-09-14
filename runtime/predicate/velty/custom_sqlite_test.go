package velty

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xpredicate "github.com/viant/xdatly/predicate"
)

type tenantPredicate struct {
	Tenant int `bind:"kind=tenant,required"`
	calls  int
}

func (p *tenantPredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.calls++
	if p.calls != 1 {
		return nil, fmt.Errorf("predicate instance was reused across invocations")
	}
	return &xpredicate.Criteria{Expression: "tenant_id = ? AND id >= ?", Placeholders: []any{p.Tenant, value}}, nil
}

type scopedPredicateBinder struct {
	predicateBinder
	injector *bindly.Injector
}

func (b scopedPredicateBinder) Bind(ctx context.Context, target any) error {
	return b.injector.Bind(ctx, target, bindly.WithSource(b.input))
}

func TestCustomPredicateScopedSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES (1, 7), (2, 7), (3, 8)"); err != nil {
		t.Fatal(err)
	}
	type input struct{ Minimum int }
	type row struct {
		ID int `sqlx:"id"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Minimum", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"example.TenantPredicate"}}}}}}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeOf(input{}), Lookup: func(string) (reflect.Type, error) { return reflect.TypeOf(tenantPredicate{}), nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name                   string
		tenant                 any
		minimum                int
		absent, canceled, fail bool
		expected               []row
	}{
		{name: "tenant seven", tenant: 7, minimum: 1, expected: []row{{1}, {2}}},
		{name: "tenant eight", tenant: 8, minimum: 1, expected: []row{{3}}},
		{name: "independent instance", tenant: 7, minimum: 2, expected: []row{{2}}},
		{name: "missing dependency", minimum: 1, fail: true},
		{name: "cancellation", tenant: 7, minimum: 1, canceled: true, fail: true},
		{name: "absent skips dependency", absent: true, expected: []row{{1}, {2}, {3}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]any{}
			if tt.tenant != nil {
				data[""] = tt.tenant
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(values.New("tenant", data)))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			var args []any
			actual, err := program.NewContext(ctx, scopedPredicateBinder{predicateBinder: predicateBinder{input: &input{Minimum: tt.minimum}}, injector: injector}, func(values ...any) { args = append(args, values...) })
			if err != nil {
				t.Fatal(err)
			}
			fragment, err := actual.(*Context).FilterGroup(0, "AND")
			if tt.fail {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			query := "SELECT id FROM records"
			if fragment != "" {
				query += " WHERE " + fragment
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query + " ORDER BY id", Args: args}, tt.expected)
		})
	}
}

func TestCustomPredicateRejectsIgnoredArguments(t *testing.T) {
	_, err := resolveHandlerType(func(string) (reflect.Type, error) { return reflect.TypeOf(tenantPredicate{}), nil }, []string{"example.Predicate", "ignored"})
	if err == nil {
		t.Fatal("ignored handler configuration accepted")
	}
}
