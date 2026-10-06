package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type queueCancelKey struct{}
type queueCancelWitness struct {
	cancel   context.CancelFunc
	calls    int
	observed error
}

var queueCancelHookAnchor = reflect.TypeFor[queueActualCancelHooks]()

type queueActualCancelHooks struct{}

func (*queueActualCancelHooks) AfterQueue(ctx context.Context, _ *policyReplayRow, _ xhandler.LifecycleContext[policyReplayRow, xhandler.NoParent, policyReplayOutput]) error {
	witness := ctx.Value(queueCancelKey{}).(*queueCancelWitness)
	witness.calls++
	witness.cancel()
	<-ctx.Done()
	witness.observed = ctx.Err()
	return nil // Cancellation must come from the actual context, not a sentinel return.
}
func TestQueueContractActualContextCancellationAfterQueue(t *testing.T) {
	_ = queueCancelHookAnchor
	for _, caller := range []bool{false, true} {
		for _, policy := range []string{"", "insert-delete"} {
			t.Run(policy+"/caller="+map[bool]string{false: "false", true: "true"}[caller], func(t *testing.T) {
				db := sqlite.New(t)
				if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
					t.Fatal(err)
				}
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "ActualCancel"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/actual-cancel"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: policy, QueueContract: "source-row", EntityHooks: "queueActualCancelHooks", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
				compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
				if err != nil {
					t.Fatal(err)
				}
				route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/actual-cancel"})
				if !ok {
					t.Fatal("missing route")
				}
				h, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				id := 1
				name := "queued"
				input := &policyReplayInput{Rows: []*policyReplayRow{{ID: &id, Name: &name, Has: &policyReplayHas{ID: true, Name: true}}}}
				source := dml.Source{DB: db.DB}
				commits := 0
				source.OnCommit = func(context.Context) { commits++ }
				if caller {
					source.Tx, err = db.DB.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer source.Tx.Rollback()
					if _, err = source.Tx.Exec("INSERT INTO records VALUES(99,'prior')"); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				witness := &queueCancelWitness{cancel: cancel}
				ctx = context.WithValue(ctx, queueCancelKey{}, witness)
				_, err = engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: input, Handler: h, DataSource: source})
				if !errors.Is(err, context.Canceled) || ctx.Err() != context.Canceled || witness.calls != 1 || witness.observed != context.Canceled || commits != 0 {
					t.Fatal("actual cancellation not honored", err, ctx.Err(), witness, commits)
				}
				if caller {
					for _, table := range []string{"records", "audit"} {
						var n int
						if err = source.Tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 1 {
							t.Fatal("caller prefix changed", table, n, err)
						}
					}
					if _, err = source.Tx.Exec("INSERT INTO records VALUES(100,'caller-usable')"); err != nil {
						t.Fatal(err)
					}
					if err = source.Tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				for _, table := range []string{"records", "audit"} {
					var n int
					if err = db.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 0 {
						t.Fatal("cancelled queue wrote SQL", table, n, err)
					}
				}
			})
		}
	}
}
