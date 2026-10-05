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
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type eligibilityTransactionProbeKey struct{}
type eligibilityTransactionProbe struct {
	db                                           *sql.DB
	queued, childCalls, pendingRoots, recoveries int
	decisions                                    []bool
	previous                                     []string
	queueCounters                                []int
	outcomes                                     []h.Outcome
	childTarget                                  dexec.ComponentTarget
}

func eligibilityTransactionProbeFrom(ctx context.Context) *eligibilityTransactionProbe {
	return ctx.Value(eligibilityTransactionProbeKey{}).(*eligibilityTransactionProbe)
}

// Each linked hook is constructed by the native writer, including on replay.
type nativeEligibilityTransactionHooks struct{ nativeEligibilityHooks }

var nativeEligibilityTransactionLinked = reflect.TypeFor[nativeEligibilityTransactionHooks]()

func (*nativeEligibilityTransactionHooks) AfterQueue(ctx context.Context, _ *nativeEligibilityRow, _ h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput]) error {
	eligibilityTransactionProbeFrom(ctx).queued++
	return nil
}
func (*nativeEligibilityTransactionHooks) Finalize(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput, outcome h.Outcome) error {
	probe := eligibilityTransactionProbeFrom(ctx)
	probe.outcomes = append(probe.outcomes, outcome)
	return nil
}

func eligibilityTransactionComponent(t *testing.T, name, method, path, table, hook string, inputType, outputType reflect.Type) *registry.RegisteredComponent {
	t.Helper()
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: nativeEligibilityTransactionLinked.PkgPath(), Name: name},
		Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: method, Path: path}},
		RootView: &spec.View{Name: "Rows", EntityHooks: hook, Source: &spec.ViewSource{Table: table}},
	}
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: inputType}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, ok := compiled.Input.ForRoute(spec.RouteRef{Method: method, Path: path})
	if !ok {
		t.Fatal("native eligibility route missing")
	}
	native, err := writer.New(component, inputType, outputType, strings.ToLower(method))
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{Component: component, Input: compiled.Input, OutputType: outputType, Handler: native}
}

func eligibilityCurrentProvider(db *sql.DB, onRead ...func()) locator.Provider {
	return handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key != "Current" {
			return nil, false, nil
		}
		for _, observe := range onRead {
			observe()
		}
		rows, err := db.QueryContext(ctx, "SELECT id,name FROM eligible ORDER BY id")
		if err != nil {
			return nil, true, err
		}
		defer rows.Close()
		var current []*nativeEligibilityRow
		for rows.Next() {
			row := &nativeEligibilityRow{ID: new(int), Name: new(string)}
			if err := rows.Scan(row.ID, row.Name); err != nil {
				return nil, true, err
			}
			current = append(current, row)
		}
		return current, true, rows.Err()
	})
}

type eligibilityNamesQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func assertEligibilityNames(t *testing.T, ctx context.Context, query eligibilityNamesQuery, table string, want []string) {
	t.Helper()
	rows, err := query.QueryContext(ctx, "SELECT name FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	actual := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		actual = append(actual, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("%s rows=%v want=%v", table, actual, want)
	}
}

func assertEligibilityTables(t *testing.T, ctx context.Context, db *sql.DB, productTables ...string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	allowed := map[string]bool{"sqlx_sequence_reservations": true}
	for _, table := range productTables {
		allowed[table] = true
	}
	seen := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if !allowed[name] {
			t.Fatalf("unexpected product/allocator table %q", name)
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, table := range productTables {
		if !seen[table] {
			t.Fatalf("missing product table %q", table)
		}
	}
}

func TestNativeWriterWriteEligibilityCallerTransactionSQLite(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("caller commit=%v", commit), func(t *testing.T) {
			db := sqlite.New(t)
			probe := &eligibilityTransactionProbe{}
			ctx := context.WithValue(context.Background(), eligibilityTransactionProbeKey{}, probe)
			if err := db.ExecStatements(ctx, `CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`, `INSERT INTO eligible(id,name) VALUES(1,'excluded old'),(2,'eligible old')`); err != nil {
				t.Fatal(err)
			}
			tx, err := db.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			commits := 0
			component := eligibilityTransactionComponent(t, "CallerEligibility", "PATCH", "/caller-eligible", "eligible", nativeEligibilityTransactionLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
			request := httptest.NewRequest("PATCH", "/caller-eligible", strings.NewReader(`{"data":[{"ID":1,"Name":"excluded requested","Locked":true},{"ID":2,"Name":"eligible requested"}]}`))
			request.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			var completion h.Outcome
			input, _ := component.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/caller-eligible"})
			output, err := engine.New().Execute(ctx, engine.Request{Input: input, Handler: component.Handler, Scope: scope, Providers: []locator.Provider{eligibilityCurrentProvider(db.DB)}, DataSource: dml.Source{DB: db.DB, Tx: tx, OnCommit: func(context.Context) { commits++ }}, Completion: func(outcome h.Outcome) { completion = outcome }})
			if err != nil {
				t.Fatal(err)
			}
			if completion.State() != h.TransactionCallerPending || completion.CommitConfirmed() || completion.Error != nil || len(completion.Transactions) != 1 || len(probe.outcomes) != 1 || probe.outcomes[0].State() != h.TransactionCallerPending || probe.queued != 1 || commits != 0 {
				t.Fatalf("caller ownership: completion=%+v hooks=%+v queued=%d runtime commits=%d", completion, probe.outcomes, probe.queued, commits)
			}
			data := output.(*nativeEligibilityOutput).Data
			if len(data) != 2 || *data[0].Name != "excluded requested" || *data[1].Name != "eligible requested" {
				t.Fatalf("excluded body was not retained in output: %+v", data)
			}
			assertEligibilityNames(t, ctx, tx, "eligible", []string{"excluded old", "eligible requested"})
			assertEligibilityNames(t, ctx, db.DB, "eligible", []string{"excluded old", "eligible old"})
			want := []string{"excluded old", "eligible old"}
			if commit {
				err = tx.Commit()
				want[1] = "eligible requested"
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatalf("caller transaction was completed by runtime: %v", err)
			}
			assertEligibilityNames(t, ctx, db.DB, "eligible", want)
			assertEligibilityTables(t, ctx, db.DB, "eligible")
		})
	}
}

type eligibilityChildHas struct{ ID, Name bool }
type eligibilityChildRow struct {
	ID   *int                 `sqlx:"id,primaryKey,autoincrement"`
	Name *string              `sqlx:"name" validate:"required"`
	Has  *eligibilityChildHas `setMarker:"true" json:"-" sqlx:"-"`
}
type eligibilityChildInput struct {
	Rows []*eligibilityChildRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=eligible_child"`
}
type eligibilityChildOutput struct {
	Data []*eligibilityChildRow `parameter:"Data,kind=output,in=body"`
}
type nativeEligibilityChildHooks struct{}

var nativeEligibilityChildLinked = reflect.TypeFor[nativeEligibilityChildHooks]()

func (*nativeEligibilityChildHooks) AfterQueue(ctx context.Context, _ *eligibilityChildRow, _ h.LifecycleContext[eligibilityChildRow, h.NoParent, eligibilityChildOutput]) error {
	eligibilityTransactionProbeFrom(ctx).childCalls++
	return nil
}

type nativeEligibilityScopedHooks struct {
	nativeEligibilityHooks
	Flusher h.Flusher              `bind:"kind=flusher,required"`
	Invoker dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
	queued  int
}

var nativeEligibilityScopedLinked = reflect.TypeFor[nativeEligibilityScopedHooks]()

func (hook *nativeEligibilityScopedHooks) AfterQueue(ctx context.Context, _ *nativeEligibilityRow, _ h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput]) error {
	hook.queued++
	probe := eligibilityTransactionProbeFrom(ctx)
	probe.queued++
	if hook.queued != 2 {
		return nil
	}
	if err := hook.Flusher.Flush(ctx, ""); err != nil {
		return err
	}
	// This proves both earlier root updates physically reached the shared pending
	// transaction before the native child is queued and its trigger fails.
	tx, err := dexec.InvocationTransaction(ctx, probe.db)
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("native root writes did not acquire a pending transaction")
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM eligible WHERE name IN ('first requested','second requested')`).Scan(&probe.pendingRoots); err != nil {
		return err
	}
	good, blocked := "child good", "child blocked"
	_, err = hook.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: probe.childTarget, Input: &eligibilityChildInput{Rows: []*eligibilityChildRow{{Name: &good, Has: &eligibilityChildHas{Name: true}}, {Name: &blocked, Has: &eligibilityChildHas{Name: true}}}}})
	return err
}
func (*nativeEligibilityScopedHooks) Finalize(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput, outcome h.Outcome) error {
	probe := eligibilityTransactionProbeFrom(ctx)
	probe.outcomes = append(probe.outcomes, outcome)
	return nil
}

func TestNativeWriterWriteEligibilityLateScopedChildRollbackSQLite(t *testing.T) {
	db := sqlite.New(t)
	probe := &eligibilityTransactionProbe{db: db.DB}
	ctx := context.WithValue(context.Background(), eligibilityTransactionProbeKey{}, probe)
	if err := db.ExecStatements(ctx,
		`CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`,
		`INSERT INTO eligible(id,name) VALUES(1,'first old'),(2,'excluded old'),(3,'second old')`,
		`CREATE TABLE eligible_child(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)`,
		`CREATE TRIGGER fail_eligibility_child BEFORE INSERT ON eligible_child WHEN NEW.name='child blocked' BEGIN SELECT RAISE(ABORT,'late scoped eligibility child fixture'); END`); err != nil {
		t.Fatal(err)
	}
	root := eligibilityTransactionComponent(t, "ScopedEligibility", "PATCH", "/scoped-eligible", "eligible", nativeEligibilityScopedLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
	child := eligibilityTransactionComponent(t, "EligibilityChild", "POST", "/eligibility-child", "eligible_child", nativeEligibilityChildLinked.Name(), reflect.TypeFor[eligibilityChildInput](), reflect.TypeFor[eligibilityChildOutput]())
	commits := 0
	root.DataSource = dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
	root.Providers = []locator.Provider{eligibilityCurrentProvider(db.DB)}
	child.DataSource = dml.Source{DB: db.DB}
	probe.childTarget = dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/eligibility-child"}}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{root, child})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	request := httptest.NewRequest("PATCH", "/scoped-eligible", strings.NewReader(`{"data":[{"ID":1,"Name":"first requested"},{"ID":2,"Name":"excluded requested","Locked":true},{"ID":3,"Name":"second requested"}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	_, err = runtime.ExecuteRoute(ctx, "PATCH", "/scoped-eligible", scope)
	if err == nil || !strings.Contains(err.Error(), "late scoped eligibility child fixture") {
		t.Fatalf("expected actual child SQL failure: %v", err)
	}
	if probe.pendingRoots != 2 || probe.queued != 2 || probe.childCalls != 2 || commits != 0 || len(probe.outcomes) != 1 || probe.outcomes[0].State() != h.TransactionRolledBack || probe.outcomes[0].CommitConfirmed() || probe.outcomes[0].Error == nil || len(probe.outcomes[0].Transactions) != 1 {
		t.Fatalf("late-child evidence: roots=%d queued=%d child queue=%d commits=%d outcomes=%+v", probe.pendingRoots, probe.queued, probe.childCalls, commits, probe.outcomes)
	}
	assertEligibilityNames(t, ctx, db.DB, "eligible", []string{"first old", "excluded old", "second old"})
	assertEligibilityNames(t, ctx, db.DB, "eligible_child", []string{})
	assertEligibilityTables(t, ctx, db.DB, "eligible", "eligible_child")
}

type nativeEligibilityRetryHooks struct{ queued int }

var nativeEligibilityRetryLinked = reflect.TypeFor[nativeEligibilityRetryHooks]()

func (*nativeEligibilityRetryHooks) WriteEligible(ctx context.Context, _ *nativeEligibilityRow, state h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput], action h.WriteAction) (bool, error) {
	if state.Previous == nil || state.Previous.Name == nil || action != h.WriteUpdate {
		return false, fmt.Errorf("retry fixture requires a matched update")
	}
	probe := eligibilityTransactionProbeFrom(ctx)
	eligible := *state.Previous.Name != "refreshed locked"
	probe.previous = append(probe.previous, *state.Previous.Name)
	probe.decisions = append(probe.decisions, eligible)
	return eligible, nil
}
func (hook *nativeEligibilityRetryHooks) AfterQueue(context.Context, *nativeEligibilityRow, h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput]) error {
	hook.queued++
	return nil
}
func (*nativeEligibilityRetryHooks) Recover(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
	if outcome.Attempt != 0 || outcome.RetryLimit != 1 || !outcome.CommitConfirmed() || outcome.Mutation.Affected != 0 || outcome.Mutation.Operation != "update" {
		return rhandler.RecoveryNone, fmt.Errorf("unexpected native replay evidence: %+v", outcome)
	}
	eligibilityTransactionProbeFrom(ctx).recoveries++
	return rhandler.RecoveryRetry, nil
}
func (hook *nativeEligibilityRetryHooks) Finalize(ctx context.Context, _ *nativeEligibilityInput, _ *nativeEligibilityOutput, outcome h.Outcome) error {
	probe := eligibilityTransactionProbeFrom(ctx)
	probe.outcomes = append(probe.outcomes, outcome)
	probe.queueCounters = append(probe.queueCounters, hook.queued)
	return nil
}

func TestNativeWriterWriteEligibilityRefreshedCurrentRetrySQLite(t *testing.T) {
	db := sqlite.New(t)
	probe := &eligibilityTransactionProbe{}
	ctx := context.WithValue(context.Background(), eligibilityTransactionProbeKey{}, probe)
	if err := db.ExecStatements(ctx,
		`CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`,
		`INSERT INTO eligible(id,name) VALUES(1,'old')`,
		`CREATE TRIGGER ignore_eligibility_update BEFORE UPDATE ON eligible WHEN NEW.name='requested' AND OLD.name='old' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	component := eligibilityTransactionComponent(t, "RetryEligibility", "PATCH", "/retry-eligible", "eligible", nativeEligibilityRetryLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
	reads, commits := 0, 0
	provider := eligibilityCurrentProvider(db.DB, func() { reads++ })
	var refreshErr error
	source := dml.Source{DB: db.DB, OnCommit: func(ctx context.Context) {
		commits++
		if commits == 1 {
			// Simulate a separate committed change between attempts, after the
			// genuine zero-row mutation has released its owning transaction.
			_, refreshErr = db.DB.ExecContext(ctx, `UPDATE eligible SET name='refreshed locked' WHERE id=1`)
		}
	}}
	request := httptest.NewRequest("PATCH", "/retry-eligible", strings.NewReader(`{"data":[{"ID":1,"Name":"requested"}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	observations := 0
	input, _ := component.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/retry-eligible"})
	output, err := engine.New().Execute(ctx, engine.Request{Input: input, Handler: component.Handler, Scope: scope, Providers: []locator.Provider{provider}, DataSource: source, Completion: func(h.Outcome) { observations++ }})
	if err != nil || refreshErr != nil {
		t.Fatalf("replay=%v fixture refresh=%v", err, refreshErr)
	}
	if reads != 2 || probe.recoveries != 1 || observations != 1 || !reflect.DeepEqual(probe.previous, []string{"old", "refreshed locked"}) || !reflect.DeepEqual(probe.decisions, []bool{true, false}) || !reflect.DeepEqual(probe.queueCounters, []int{1, 0}) || len(probe.outcomes) != 2 || probe.outcomes[0].Error == nil || probe.outcomes[1].Error != nil {
		t.Fatalf("retry evidence: reads=%d recoveries=%d observations=%d previous=%v decisions=%v hook queue=%v outcomes=%+v", reads, probe.recoveries, observations, probe.previous, probe.decisions, probe.queueCounters, probe.outcomes)
	}
	data := output.(*nativeEligibilityOutput).Data
	if len(data) != 1 || data[0].ID == nil || *data[0].ID != 1 || data[0].Name == nil || *data[0].Name != "requested" {
		t.Fatalf("replay lost original requested body: %+v", data)
	}
	assertEligibilityNames(t, ctx, db.DB, "eligible", []string{"refreshed locked"})
	assertEligibilityTables(t, ctx, db.DB, "eligible")
}
