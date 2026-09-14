package reader

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

type customPredicateInput struct {
	Minimum int `parameter:"Minimum,kind=query,in=min,required" predicate:"handler,example.ScopedPredicate"`
	Maximum int `parameter:"Maximum,kind=query,in=max,required"`
}

type scopedReaderPredicate struct {
	Tenant         int                   `bind:"kind=tenant,required"`
	ComponentInput *customPredicateInput `bind:"kind=input,required"`
	calls          int
}

func (p *scopedReaderPredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.calls++
	if p.calls != 1 {
		return nil, fmt.Errorf("predicate state leaked between reads")
	}
	if p.ComponentInput == nil || p.ComponentInput.Minimum != value {
		return nil, fmt.Errorf("canonical component input was not bound")
	}
	return &xpredicate.Criteria{Expression: "tenant_id = ? AND id >= ? AND id <= ?", Placeholders: []any{p.Tenant, value, p.ComponentInput.Maximum}}, nil
}

func TestReaderCustomPredicateGoShapeSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES (1, 7), (2, 7), (3, 8)"); err != nil {
		t.Fatal(err)
	}
	type input = customPredicateInput
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	component := &spec.Component{Name: "CustomPredicate", RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data", Types: testTypeCatalog(t, map[string]reflect.Type{"example.ScopedPredicate": reflect.TypeOf(scopedReaderPredicate{})})})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name            string
		tenant, minimum int
		maximum         int
		missing         bool
		expected        []row
	}{
		{"tenant seven", 7, 1, 3, false, []row{{1}, {2}}},
		{"tenant eight", 8, 1, 3, false, []row{{3}}},
		{"repeat isolated", 7, 2, 3, false, []row{{2}}},
		{"other input field controls predicate", 7, 1, 1, false, []row{{1}}},
		{"missing tenant", 0, 1, 3, true, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var providers []locator.Provider
			if !tt.missing {
				providers = append(providers, handlerprovider.Static(xhandler.ValueKey("tenant"), tt.tenant))
			}
			actual, err := NewService().Read(context.Background(), &Session{Component: artifact.Component, Input: routeInput(t, artifact), OutputType: reflect.TypeOf(output{}), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{"min": {strconv.Itoa(tt.minimum)}, "max": {strconv.Itoa(tt.maximum)}}), Providers: providers})
			if tt.missing {
				if err == nil {
					t.Fatal("missing tenant accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual.(*output).Data, tt.expected) {
				t.Fatalf("actual=%#v want=%#v", actual, tt.expected)
			}
		})
	}
}
