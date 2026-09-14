package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type predicateAccessInput struct {
	Tenant int `parameter:"Tenant,kind=query,in=tenant,required"`
}
type predicateAccessOutput struct{ Tenant int }
type dependentPredicate struct {
	Access *predicateAccessOutput `bind:"kind=component,in=GET:/predicate-access,required"`
}

func (p *dependentPredicate) Compute(_ context.Context, value any) (*xpredicate.Criteria, error) {
	if p.Access == nil {
		return nil, fmt.Errorf("component result missing")
	}
	return &xpredicate.Criteria{Expression: "tenant_id = ? AND id >= ?", Placeholders: []any{p.Access.Tenant, value}}, nil
}

func TestCustomPredicateBindsComponentResultSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES (1, 7), (2, 7), (3, 8)"); err != nil {
		t.Fatal(err)
	}
	type input struct {
		Minimum int `parameter:"Minimum,kind=query,in=min,required" predicate:"handler,example.DependentPredicate"`
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	child := componentArtifact(t, componentSpec("PredicateAccess", "GET", "/predicate-access", nil), reflect.TypeOf(predicateAccessInput{}), reflect.TypeOf(predicateAccessOutput{}))
	parent := componentSpec("PredicateRecords", "GET", "/predicate-records", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	parent.RootView = &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(dependentPredicate{}), PkgPath: "example", Name: "DependentPredicate"}); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parent, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data", Types: types})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: child.Component, Input: child.Input, OutputType: reflect.TypeOf(predicateAccessOutput{}), Handler: customhandler.NewFunc[predicateAccessInput, predicateAccessOutput](func(_ context.Context, input *predicateAccessInput) (*predicateAccessOutput, error) {
			calls++
			if input.Tenant == 9 {
				return nil, fmt.Errorf("access denied")
			}
			return &predicateAccessOutput{Tenant: input.Tenant}, nil
		})},
		{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		tenant   int
		fail     bool
		expected []row
	}{{7, false, []row{{1}, {2}}}, {8, false, []row{{3}}}, {9, true, nil}} {
		t.Run(strconv.Itoa(tt.tenant), func(t *testing.T) {
			before := calls
			actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest(http.MethodGet, "/predicate-records").WithQuery(url.Values{"tenant": {strconv.Itoa(tt.tenant)}, "min": {"1"}}))
			if calls != before+1 {
				t.Fatalf("child calls=%d, expected one", calls-before)
			}
			if tt.fail {
				if err == nil {
					t.Fatal("child error ignored")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual.(*output).Data, tt.expected) {
				t.Fatalf("actual=%#v expected=%#v", actual, tt.expected)
			}
		})
	}
}
