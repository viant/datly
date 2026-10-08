package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type afterQueueInputProbeKey struct{}
type afterQueueInputProbe struct {
	db       *sql.DB
	mode     string
	trace    []string
	target   dexec.ComponentTarget
	outcomes []h.Outcome
}

type batchRecoveryHooks struct{ nativeEligibilityRetryHooks }

var batchRecoveryLinked = reflect.TypeFor[batchRecoveryHooks]()

func (*batchRecoveryHooks) AfterQueueInput(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput) error {
	p := afterQueueInputProbeFrom(ctx)
	p.trace = append(p.trace, "batch")
	return nil
}

// A real zero-affected mutation normally exercises the existing Recover hook.
// Entering the batch boundary vetoes that replay even after confirmed commit.
func TestAfterQueueInputVetoesNativeRecoverySQLite(t *testing.T) {
	db := sqlite.New(t)
	p := &afterQueueInputProbe{}
	prior := &eligibilityTransactionProbe{}
	ctx := context.WithValue(context.WithValue(context.Background(), afterQueueInputProbeKey{}, p), eligibilityTransactionProbeKey{}, prior)
	if err := db.ExecStatements(ctx, `CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`, `INSERT INTO eligible(id,name) VALUES(1,'old')`, `CREATE TRIGGER ignore_batch_update BEFORE UPDATE ON eligible WHEN NEW.name='requested' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	component := eligibilityTransactionComponent(t, "BatchRecovery", "PATCH", "/batch-recovery", "eligible", batchRecoveryLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
	request := httptest.NewRequest("PATCH", "/batch-recovery", strings.NewReader(`{"data":[{"ID":1,"Name":"requested"}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	reads, commits, observations := 0, 0, 0
	input, _ := component.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/batch-recovery"})
	_, err = engine.New().Execute(ctx, engine.Request{Input: input, Handler: component.Handler, Scope: scope, Providers: []locator.Provider{eligibilityCurrentProvider(db.DB, func() { reads++ })}, DataSource: dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}, Completion: func(h.Outcome) { observations++ }})
	if err != nil || reads != 1 || commits != 1 || observations != 1 || prior.recoveries != 0 || len(prior.outcomes) != 1 || !prior.outcomes[0].CommitConfirmed() || prior.outcomes[0].Error != nil || !reflect.DeepEqual(p.trace, []string{"batch"}) || !reflect.DeepEqual(prior.queueCounters, []int{1}) {
		t.Fatalf("err=%v reads=%d commits=%d observations=%d recoveries=%d trace=%v counters=%v outcomes=%+v", err, reads, commits, observations, prior.recoveries, p.trace, prior.queueCounters, prior.outcomes)
	}
	assertEligibilityNames(t, ctx, db.DB, "eligible", []string{"old"})
}

func afterQueueInputProbeFrom(ctx context.Context) *afterQueueInputProbe {
	return ctx.Value(afterQueueInputProbeKey{}).(*afterQueueInputProbe)
}

type bufferedBatchHooks struct {
	nativeEligibilityHooks
	Invoker dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
}

var bufferedBatchLinked = reflect.TypeFor[bufferedBatchHooks]()

func (*bufferedBatchHooks) AfterQueue(ctx context.Context, row *nativeEligibilityRow, _ h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput]) error {
	p := afterQueueInputProbeFrom(ctx)
	p.trace = append(p.trace, fmt.Sprintf("root:%d", *row.ID))
	return nil
}
func (hook *bufferedBatchHooks) AfterQueueInput(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput) error {
	p := afterQueueInputProbeFrom(ctx)
	p.trace = append(p.trace, "batch")
	name := "child queued"
	row := &eligibilityChildRow{Name: &name, Has: &eligibilityChildHas{Name: true}}
	if p.mode == "swallowed child failure" {
		row.Name = nil
	}
	_, err := hook.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: p.target, Input: &eligibilityChildInput{Rows: []*eligibilityChildRow{row}}})
	if err != nil && p.mode != "swallowed child failure" {
		return err
	}
	// A successful buffered child call must not drain the shared owner.
	var count int
	if err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM eligible_child").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("buffered child drained before completion: %d", count)
	}
	if err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM eligible WHERE name LIKE '%requested'").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("buffered parent drained before completion: %d", count)
	}
	if p.mode == "late boundary failure" {
		return fmt.Errorf("late batch boundary fixture")
	}
	return nil
}
func (*bufferedBatchHooks) Finalize(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput, outcome h.Outcome) error {
	p := afterQueueInputProbeFrom(ctx)
	p.trace = append(p.trace, "finalize")
	p.outcomes = append(p.outcomes, outcome)
	return nil
}

type bufferedBatchChildHooks struct{}

var bufferedBatchChildLinked = reflect.TypeFor[bufferedBatchChildHooks]()

func (*bufferedBatchChildHooks) AfterQueue(ctx context.Context, _ *eligibilityChildRow, _ h.LifecycleContext[eligibilityChildRow, h.NoParent, eligibilityChildOutput]) error {
	p := afterQueueInputProbeFrom(ctx)
	p.trace = append(p.trace, "child")
	return nil
}

func TestAfterQueueInputBufferedOwnerSQLite(t *testing.T) {
	for _, mode := range []string{"success", "suppressed roots", "late boundary failure", "swallowed child failure"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			p := &afterQueueInputProbe{db: db.DB, mode: mode}
			ctx := context.WithValue(context.Background(), afterQueueInputProbeKey{}, p)
			if err := db.ExecStatements(ctx, `CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`, `INSERT INTO eligible(id,name) VALUES(1,'first old'),(2,'second old')`, `CREATE TABLE eligible_child(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			root := eligibilityTransactionComponent(t, "BufferedBatch", "PATCH", "/buffered-batch", "eligible", bufferedBatchLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
			child := eligibilityTransactionComponent(t, "BufferedBatchChild", "POST", "/buffered-batch-child", "eligible_child", bufferedBatchChildLinked.Name(), reflect.TypeFor[eligibilityChildInput](), reflect.TypeFor[eligibilityChildOutput]())
			root.Component.Settings.ComponentCallPolicy = "buffered"
			commits := 0
			root.DataSource = dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
			root.Providers = []locator.Provider{eligibilityCurrentProvider(db.DB)}
			child.DataSource = dml.Source{DB: db.DB}
			p.target = dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/buffered-batch-child"}}
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{root, child})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Shutdown(context.Background())
			body := `{"data":[{"ID":1,"Name":"first requested"},{"ID":2,"Name":"second requested"}]}`
			if mode == "suppressed roots" {
				body = `{"data":[{"ID":1,"Name":"first requested","Locked":true},{"ID":2,"Name":"second requested","Locked":true}]}`
			}
			request := httptest.NewRequest("PATCH", "/buffered-batch", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			_, err = runtime.ExecuteRoute(ctx, "PATCH", "/buffered-batch", scope)
			wantTrace := []string{"root:1", "root:2", "batch", "child", "finalize"}
			if mode == "swallowed child failure" {
				wantTrace = []string{"root:1", "root:2", "batch", "finalize"}
			}
			if mode == "suppressed roots" {
				wantTrace = []string{"batch", "child", "finalize"}
			}
			if !reflect.DeepEqual(p.trace, wantTrace) {
				t.Fatalf("trace=%v want=%v", p.trace, wantTrace)
			}
			if mode == "success" || mode == "suppressed roots" {
				if err != nil || commits != 1 || len(p.outcomes) != 1 || !p.outcomes[0].CommitConfirmed() {
					t.Fatalf("success err=%v commits=%d outcomes=%+v", err, commits, p.outcomes)
				}
				wantRoots := []string{"first requested", "second requested"}
				if mode == "suppressed roots" {
					wantRoots = []string{"first old", "second old"}
				}
				assertEligibilityNames(t, ctx, db.DB, "eligible", wantRoots)
				assertEligibilityNames(t, ctx, db.DB, "eligible_child", []string{"child queued"})
			} else {
				if err == nil || commits != 0 || len(p.outcomes) != 1 || p.outcomes[0].Error == nil || p.outcomes[0].CommitConfirmed() {
					t.Fatalf("failure err=%v commits=%d outcomes=%+v", err, commits, p.outcomes)
				}
				assertEligibilityNames(t, ctx, db.DB, "eligible", []string{"first old", "second old"})
				assertEligibilityNames(t, ctx, db.DB, "eligible_child", []string{})
			}
		})
	}
}
