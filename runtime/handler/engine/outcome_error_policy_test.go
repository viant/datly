package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

func TestOutcomePublicErrorNestedPolicy(t *testing.T) {
	validation := &xhandler.Validation{Failed: true, Violations: []*xhandler.Violation{{Field: "Name", Message: "required"}}}
	private := errors.New("private cause")
	cleanup := errors.New("cleanup failed")
	childPublic := &response.Error{Code: 409, Payload: "child", Cause: private}
	parentPublic := &response.Error{Code: 401, Payload: map[string]any{"message": "", "error": nil}, Cause: validation}
	parentValidation := &xhandler.Validation{Code: 403, Failed: true}
	wrapped := fmt.Errorf("cleanup: %w", validation)
	for _, tc := range []struct {
		name          string
		child, parent error
		want          response.BodyError
	}{
		{"unchanged", nil, nil, validation},
		{"plain cleanup", cleanup, wrapped, validation},
		{"child mapping", childPublic, nil, childPublic},
		{"parent cleanup", childPublic, cleanup, childPublic},
		{"parent wraps operation", childPublic, wrapped, childPublic},
		{"parent returns operation", childPublic, validation, childPublic},
		{"parent maps", childPublic, parentPublic, parentPublic},
		{"new typed public validation", childPublic, parentValidation, parentValidation},
		{"wrapped parent maps", childPublic, fmt.Errorf("policy: %w", parentPublic), parentPublic},
		{"joined parent maps", childPublic, errors.Join(wrapped, cleanup, parentPublic), parentPublic},
		{"joined cleanup", childPublic, errors.Join(cleanup, wrapped), childPublic},
		{"null body", childPublic, &response.Error{Code: 403}, &response.Error{Code: 403}},
		{"empty body", nil, &response.Error{Code: 400, Payload: ""}, &response.Error{Code: 400, Payload: ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			input := testRouteInput(t, reflect.TypeOf(struct{}{}))
			finalize := func(name string, failure error) func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error {
				return func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
					order = append(order, name)
					if !errors.Is(outcome.Error, validation) || outcome.CommitConfirmed() {
						t.Fatalf("lost control failure: %+v", outcome)
					}
					return failure
				}
			}
			child := &outcomeAwareHandler{execute: func(context.Context, rhandler.Invocation) (any, error) {
				return nil, validation
			}, finalize: finalize("child", tc.child)}
			parent := &outcomeAwareHandler{execute: func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				return New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: input, Handler: child})
			}, finalize: finalize("parent", tc.parent)}
			_, err := New().Execute(context.Background(), Request{Input: input, Handler: parent})
			var original *xhandler.Validation
			if !errors.Is(err, validation) || !errors.As(err, &original) || original != validation {
				t.Fatalf("original validation lost: %v", err)
			}
			body, explicit := response.ErrorBody(err)
			if !explicit || !reflect.DeepEqual(body, tc.want.ResponseBody()) || response.ErrorStatusCode(err, 0) != tc.want.StatusCode() {
				t.Fatalf("projection status=%d body=%#v error=%v", response.ErrorStatusCode(err, 0), body, err)
			}
			if !reflect.DeepEqual(order, []string{"child", "parent"}) {
				t.Fatalf("callback order=%v", order)
			}
			var diagnostic *FinalizationError
			wantDiagnostic := tc.child != nil || tc.parent != nil
			if errors.As(err, &diagnostic) != wantDiagnostic {
				t.Fatalf("finalization diagnostic=%v", err)
			}
			if diagnostic != nil && (!errors.Is(diagnostic.Outcome.Error, validation) || diagnostic.Outcome.CommitConfirmed()) {
				t.Fatalf("diagnostic outcome=%+v", diagnostic.Outcome)
			}
			for _, failure := range []error{tc.child, tc.parent} {
				if failure != nil && !errors.Is(err, failure) {
					t.Fatalf("callback error lost: %v", failure)
				}
			}
			if tc.child == childPublic && !errors.Is(err, private) {
				t.Fatal("private diagnostic cause lost")
			}
		})
	}
}

func TestOutcomePublicErrorTransactionEvidenceSQLite(t *testing.T) {
	for _, failed := range []bool{true, false} {
		t.Run(fmt.Sprintf("validation_failure_%v", failed), func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			validation := &xhandler.Validation{Failed: true}
			public := &response.Error{Code: 401, Payload: nil}
			wantRows, wantState := 1, xhandler.TransactionCommitted
			if failed {
				wantRows, wantState = 0, xhandler.TransactionRolledBack
			}
			calls := 0
			adapter := &outcomeAwareHandler{execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				data := value.(xhandler.Data)
				if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
					return nil, err
				}
				if err := data.Flush(ctx, ""); err != nil {
					return nil, err
				}
				if failed {
					return nil, validation
				}
				return nil, nil
			}, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
				calls++
				if outcome.State() != wantState || errors.Is(outcome.Error, validation) != failed {
					t.Fatalf("actual outcome=%+v", outcome)
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{wantRows}})
				return public
			}}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: dml.Source{DB: h.DB}, Handler: adapter})
			var diagnostic *FinalizationError
			if calls != 1 || !errors.Is(err, public) || !errors.As(err, &diagnostic) || diagnostic.Outcome.State() != wantState || errors.Is(err, validation) != failed {
				t.Fatalf("calls=%d error=%v diagnostic=%+v", calls, err, diagnostic)
			}
			body, explicit := response.ErrorBody(err)
			if !explicit || body != nil || response.ErrorStatusCode(err, 0) != 401 {
				t.Fatalf("projection=%#v error=%v", body, err)
			}
		})
	}
}
