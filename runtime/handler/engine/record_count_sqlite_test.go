package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

func TestEngineRecordCountPolicySQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records (id INTEGER PRIMARY KEY)`, `INSERT INTO records VALUES (1), (2), (3)`); err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type input struct {
		Rows []row `parameter:"kind=view,in=records"`
	}
	one, two := 1, 2
	for _, tt := range []struct {
		name            string
		count           int
		min, max, exact *int
		reject          bool
	}{
		{"minimum rejected", 0, &one, nil, nil, true},
		{"minimum accepted", 1, &one, nil, nil, false},
		{"maximum accepted", 2, nil, &two, nil, false},
		{"maximum rejected", 3, nil, &two, nil, true},
		{"exact accepted", 2, nil, nil, &two, false},
		{"exact rejected", 1, nil, nil, &two, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "records"}, MinAllowedRecords: tt.min, MaxAllowedRecords: tt.max, ExpectedReturned: tt.exact, ErrorStatusCode: 422, ErrorMessage: "invalid records"}}}
			bindings, err := compiler.BuildBindingSpecs(component, reflect.TypeOf(input{}), nil)
			if err != nil {
				t.Fatal(err)
			}
			provider := handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
				rows, err := h.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id FROM records WHERE id <= ? ORDER BY id", Args: []any{tt.count}}, reflect.TypeOf([]row{}))
				return rows, true, err
			})
			called := false
			_, err = New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(input{}), bindings...), Scope: engineProviderScope{provider}, Handler: rhandler.HandlerFunc(func(_ context.Context, invocation rhandler.Invocation) (any, error) {
				called = true
				if len(invocation.Input.(*input).Rows) != tt.count {
					t.Fatal("typed rows lost")
				}
				return nil, nil
			})})
			if tt.reject {
				var bindingError *bindly.BindingError
				if called || !errors.As(err, &bindingError) || bindingError.StatusCode() != 422 || bindingError.Error() != "invalid records" {
					t.Fatalf("called=%t error=%v", called, err)
				}
			} else if err != nil || !called {
				t.Fatalf("called=%t error=%v", called, err)
			}
		})
	}
}
