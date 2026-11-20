package writer

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
)

// Input views provide the scope and membership snapshot. Inspect the live
// transaction at binding time; a transaction opened in Input.Init is too late.
func TestAssociationDeltaIsolationBeforeCurrentSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                string
		auxiliary, external bool
	}{
		{"direct", false, false}, {"auxiliary parent with writable child", true, false}, {"unverifiable caller transaction", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(t.Context(), sqSchema, "INSERT INTO parents(id,name) VALUES(1,'one')"))
			c := sqComponent("association", "patch", "")
			ref := spec.RouteRef{Method: "PATCH", Path: "/parents"}
			c.Routes = []*spec.Route{{Method: ref.Method, Path: ref.Path}}
			c.RootView.Auxiliary = tc.auxiliary
			inputType := reflect.TypeFor[associationInput]()
			native, err := New(c, inputType, reflect.TypeFor[associationOutput](), "patch")
			require.NoError(t, err)
			require.True(t, native.RequiresPreBindingTransaction())

			tool := compileAssociationTool(t, c)
			ctx, scope, err := tool.AssociationScope(t.Context(), map[string]any{"data": []any{map[string]any{"id": 1, "labelsToBeAdded": []any{"new"}}}})
			require.NoError(t, err)
			defer scope.Close()
			isolation, ok := exec.RequestedTransactionIsolation(ctx)
			require.True(t, ok)
			require.Equal(t, exec.IsolationSerializable, isolation)
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			bindings := []bindly.BindingSpec{{Path: "Rows", Location: state.Location{Kind: "body", In: "data"}}, {Path: "CurrentRows", Location: state.Location{Kind: "view", In: "CurrentRows"}}}
			plan, err := injector.CompilePlan(inputType, bindings...)
			require.NoError(t, err)
			projection, err := plan.Projection()
			require.NoError(t, err)
			contract, err := registry.NewInputContract(inputType, projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: bindings})
			require.NoError(t, err)
			route, ok := contract.ForRoute(ref)
			require.True(t, ok)
			reads := 0
			stop := errors.New("stop during authorized membership read")
			views := provider.Named("view", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
				reads++
				tx, err := exec.InvocationTransaction(ctx, db.DB)
				if err != nil {
					return nil, false, err
				}
				if tx == nil {
					return nil, false, errors.New("Current opened before owned transaction")
				}
				var id int
				if err := tx.QueryRowContext(ctx, "SELECT id FROM parents WHERE id=1").Scan(&id); err != nil {
					return nil, false, err
				}
				return nil, false, stop
			})
			source := dml.Source{DB: db.DB}
			if tc.external {
				source.Tx, err = db.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
				require.NoError(t, err)
				defer source.Tx.Rollback()
			}
			_, err = engine.New().Execute(ctx, engine.Request{Input: route, Scope: scope, Handler: native, Providers: []locator.Provider{views}, DataSource: source})
			if tc.external {
				require.ErrorContains(t, err, "existing transaction")
				require.Zero(t, reads)
			} else {
				require.ErrorContains(t, err, stop.Error())
				require.Equal(t, 1, reads)
			}
		})
	}
}
