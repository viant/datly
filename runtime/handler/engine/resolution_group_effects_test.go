package engine

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/sql/dml"
	"testing"
)

func TestResolutionGroupOpenDeniesRealManagedEffectsAndNativeCompletion(t *testing.T) {
	for _, operation := range []string{"Insert", "Update", "Delete", "Execute", "Allocate", "Reserve", "Flush", "Start", "PrepareFinalization", "PrepareCompletion", "Complete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := t.Context()
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			scope := newDataScope(dml.Source{DB: h.DB})
			if err := scope.enrollBufferedScope(ctx); err != nil {
				t.Fatal(err)
			}
			data, err := scope.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			native := data.(*dml.Data)
			if err = data.Execute("INSERT INTO records VALUES(101,'existing queued')"); err != nil {
				t.Fatal(err)
			}
			group, err := drainowner.OpenBindingGroup(scope.nativeInvocation, []drainowner.BindingGroupMember{{Path: "path"}})
			if err != nil {
				t.Fatal(err)
			}
			capabilities := newHandlerData(data, scope.mutationGuard())
			row := &struct {
				ID   int    `sqlx:"id,primaryKey"`
				Name string `sqlx:"name"`
			}{ID: 1, Name: "forbidden"}
			switch operation {
			case "Insert":
				err = capabilities.Insert("records", row)
			case "Update":
				err = capabilities.Update("records", row)
			case "Delete":
				err = capabilities.Delete("records", row)
			case "Execute":
				err = capabilities.Execute("INSERT INTO records VALUES(1,'forbidden')")
			case "Allocate":
				err = capabilities.Allocate(ctx, "records", row, "ID")
			case "Reserve":
				err = capabilities.Reserve(ctx, "records", row, "ID")
			case "Flush":
				err = capabilities.Flush(ctx, "")
			case "Start":
				err = native.Start(ctx)
			case "PrepareFinalization":
				err = native.PrepareFinalization(ctx)
			case "PrepareCompletion":
				err = native.PrepareCompletion(ctx)
			case "Complete":
				err = native.Complete(ctx, nil)
			}
			if err == nil {
				t.Fatalf("open group allowed actual %s", operation)
			}
			// Catching this denial cannot authorize the remaining group or root flush.
			if !errors.Is(drainowner.ProtectedFailure(scope.nativeInvocation), err) {
				t.Fatalf("denial not retained immediately: %v", err)
			}
			if _, enterErr := group.Enter(context.Background(), "path"); enterErr == nil {
				t.Fatal("caught effect denial permitted another member")
			}
			if closeErr := group.Close(); closeErr == nil {
				t.Fatal("terminal denial disappeared on group close")
			}
			if _, tx := native.InvocationTransaction(); tx != nil {
				t.Fatal("forbidden operation started an owned transaction")
			}
			if completeErr := completeDataScope(ctx, scope, true, err); completeErr == nil {
				t.Fatal("failed root completed successfully")
			}
			var count int
			if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&count); err != nil || count != 0 {
				t.Fatalf("forbidden or queued effect executed count%d err%v", count, err)
			}
		})
	}
}
