package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type policyReplayHas struct{ ID, Name, Remove bool }
type policyReplayRow struct {
	ID     *int             `sqlx:"id,primaryKey=true" json:"id,omitempty"`
	Name   *string          `sqlx:"name" json:"name,omitempty"`
	Remove bool             `sqlx:"-" writer:"delete" json:"remove,omitempty"`
	Has    *policyReplayHas `setMarker:"true" sqlx:"-" json:"-"`
}
type policyReplayInput struct {
	Rows        []*policyReplayRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	CurrentRows []*policyReplayRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records"`
}
type policyReplayOutput struct {
	Data []*policyReplayRow `parameter:"Data,kind=output,in=body"`
}

func TestInsertDeleteTypedCallerSourceReplay(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO records VALUES(1,'old')", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES('delete');END", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "PolicyReplay"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/policy-replay"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/policy-replay"})
	if !ok {
		t.Fatal("route missing")
	}
	native, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	id, requested := 1, "requested"
	body := []*policyReplayRow{{ID: &id, Remove: true, Has: &policyReplayHas{ID: true, Remove: true}}, {ID: &id, Name: &requested, Has: &policyReplayHas{ID: true, Name: true}}}
	bodyReads, reads, commits := 0, 0, 0
	previous := []string{}
	typed := handlerprovider.Named("body", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key != "data" {
			return nil, false, nil
		}
		bodyReads++
		return body, true, nil
	})
	current := handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key != "CurrentRows" {
			return nil, false, nil
		}
		reads++
		value := &policyReplayRow{ID: new(int), Name: new(string)}
		err := db.DB.QueryRowContext(ctx, "SELECT id,name FROM records WHERE id=1").Scan(value.ID, value.Name)
		if err == nil {
			previous = append(previous, *value.Name)
		}
		return []*policyReplayRow{value}, true, err
	})
	replayPlan, err := route.Plan().Replay("Rows")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := replayPlan.CaptureSources(ctx, []locator.Provider{typed})
	if err != nil {
		t.Fatal(err)
	}
	source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}

	first, err := engine.New().Execute(ctx, engine.Request{Input: route, Handler: native, Providers: []locator.Provider{typed, current}, DataSource: source})
	if err != nil {
		t.Fatal(err)
	}
	beforeReplayReads := bodyReads
	// Mutate the caller-owned source and its markers after capture. Replay must
	// restore its detached original DELETE/INSERT partition and physical identity.
	*body[0].ID = 9
	body[0].Remove = false
	body[0].Has.Remove = false
	body[1].Name = new(string)
	*body[1].Name = "tampered"
	body[1].Has.Name = false
	if _, err = db.DB.ExecContext(ctx, "UPDATE records SET name='refreshed' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	second, err := engine.New().Execute(ctx, engine.Request{Input: route, Handler: native, Providers: []locator.Provider{typed, current}, Replay: &bindly.ReplayBinding{Replay: replay}, DataSource: source})
	if err != nil {
		t.Fatal(err)
	}
	if bodyReads != beforeReplayReads || reads != 2 || commits != 2 || !reflect.DeepEqual(previous, []string{"old", "refreshed"}) {
		t.Fatalf("source replay reads=%d/%d current=%d commits=%d previous=%v", bodyReads, beforeReplayReads, reads, commits, previous)
	}
	if first == second {
		t.Fatal("output reused")
	}
	out := second.(*policyReplayOutput)
	if len(out.Data) != 2 || out.Data[0].ID == nil || *out.Data[0].ID != 1 || !out.Data[0].Remove || out.Data[0].Has == nil || !out.Data[0].Has.Remove || out.Data[0].Has.Name || !out.Data[0].Has.ID || out.Data[1].Has.Remove || !out.Data[1].Has.ID || out.Data[1].Name == nil || *out.Data[1].Name != "requested" || !out.Data[1].Has.Name {
		t.Fatalf("captured source/markers lost: delete=%+v insert=%+v", out.Data[0].Has, out.Data[1].Has)
	}
	var name string
	var rows, events int
	if err = db.DB.QueryRowContext(ctx, "SELECT name FROM records WHERE id=1").Scan(&name); err != nil || name != "requested" {
		t.Fatalf("stored=%q err=%v", name, err)
	}
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 4 {
		t.Fatalf("events=%d err=%v", events, err)
	}
	auditRows, err := db.DB.Query("SELECT action FROM audit ORDER BY rowid")
	if err != nil {
		t.Fatal(err)
	}
	defer auditRows.Close()
	for _, expected := range []string{"delete", "insert", "delete", "insert"} {
		var actual string
		if !auditRows.Next() {
			t.Fatal("audit event missing")
		}
		if err = auditRows.Scan(&actual); err != nil || actual != expected {
			t.Fatalf("audit action=%q expected=%q err=%v", actual, expected, err)
		}
	}
	if auditRows.Next() {
		t.Fatal("extra audit event")
	}
	if err = auditRows.Err(); err != nil {
		t.Fatal(err)
	}
}

// A composing parent keeps the native child buffer until root completion.
// Its post-child callback must not retarget an already admitted physical action.
func TestInsertDeleteComposingParentRetainedPhysicalIdentity(t *testing.T) {
	for _, mode := range []string{"ID", "delete", "presence", "slot", "DataFlush", "FocusedFlush", "imperative", "finalizer", "childEnd", "outputBinding", "caller"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO records VALUES(1,'authorized'),(2,'unrelated')", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES('delete');END"); err != nil {
				t.Fatal(err)
			}
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "RetainedChild"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/retained-child"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
			compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			childRoute, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/retained-child"})
			if !ok {
				t.Fatal("child route missing")
			}
			parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "RetainedParent"}, Routes: []*spec.Route{{Method: "POST", Path: "/retained-parent"}}}
			parentCompiled, err := compiler.New(compiler.Input{Component: parent, InputType: reflect.TypeFor[struct{}]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			parentRoute, ok := parentCompiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/retained-parent"})
			if !ok {
				t.Fatal("parent route missing")
			}
			native, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			id := 1
			input := &policyReplayInput{Rows: []*policyReplayRow{{ID: &id, Remove: true, Has: &policyReplayHas{ID: true, Remove: true}}}, CurrentRows: []*policyReplayRow{{ID: &id}}}
			source := dml.Source{DB: db.DB}
			var callerTx *sql.Tx
			var callerOutcome xhandler.Outcome
			if mode == "caller" {
				callerTx, err = db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer callerTx.Rollback()
				source.Tx = callerTx
			}
			childReturned := false
			finalizerCalls := 0
			childEndCalls, outputBindingCalls := 0, 0
			_, err = engine.New().Execute(ctx, engine.Request{Input: parentRoute, BoundInput: &struct{}{}, DataSource: source, Completion: func(outcome xhandler.Outcome) { callerOutcome = outcome }, Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				childContext := ctx
				if mode == "imperative" {
					childContext = engine.PrepareComponent(ctx, engine.ComponentImperative, "")
				}
				var childHandler rhandler.Handler = native
				if mode == "childEnd" {
					childHandler = &policyObservedWriter{Handler: native, observer: &policyInvocationEndObserver{ended: func() { childEndCalls++; *input.Rows[0].ID = 2 }}}
				}
				childRequest := engine.Request{Input: childRoute, BoundInput: input, Handler: childHandler, DataSource: source}
				if mode == "outputBinding" {
					injector, e := bindly.NewInjector()
					if e != nil {
						return nil, e
					}
					plan, e := injector.CompilePlan(reflect.TypeFor[policyReplayOutput](), bindly.BindingSpec{Path: "Data", Name: "Data", Location: bindstate.Location{Kind: "output_probe", In: "rows"}})
					if e != nil {
						return nil, e
					}
					childRequest.OutputCapabilities = plan
					childRequest.Providers = []locator.Provider{handlerprovider.Named("output_probe", func(context.Context, reflect.Type, string) (any, bool, error) {
						outputBindingCalls++
						*input.Rows[0].ID = 2
						return input.Rows, true, nil
					})}
				}
				result, childErr := engine.New().Execute(childContext, childRequest)
				if childErr != nil {
					return nil, childErr
				}
				childReturned = true
				row := result.(*policyReplayOutput).Data[0]
				if mode == "finalizer" {
					return &policyParentFinalizer{finalize: func(cause error) error {
						finalizerCalls++
						if cause != nil {
							return cause
						}
						*row.ID = 2
						return nil
					}}, nil
				}
				switch mode {
				case "childEnd", "outputBinding":
				case "delete":
					row.Remove = false
				case "presence":
					row.Has.Remove = false
				case "slot":
					input.Rows[0] = &policyReplayRow{ID: row.ID, Remove: true, Has: &policyReplayHas{ID: true, Remove: true}}
				default:
					*row.ID = 2
				}
				if mode == "DataFlush" || mode == "FocusedFlush" {
					key := xhandler.DataKey
					if mode == "FocusedFlush" {
						key = xhandler.FlusherKey
					}
					capability, found, lookupErr := invocation.Binder.Lookup(ctx, key)
					if lookupErr != nil || !found {
						return nil, fmt.Errorf("flush capability unavailable: %v", lookupErr)
					}
					// Deliberately swallow the failed flush: the journal must remain poisoned.
					_ = capability.(xhandler.Flusher).Flush(ctx, "records")
				}
				return result, nil
			})})
			if !childReturned {
				t.Fatalf("native child never returned: %v", err)
			}
			if mode == "childEnd" && childEndCalls != 1 {
				t.Fatalf("child invocation-end calls=%d", childEndCalls)
			}
			if mode == "outputBinding" && outputBindingCalls != 1 {
				t.Fatalf("output binding calls=%d", outputBindingCalls)
			}
			if mode == "finalizer" && finalizerCalls != 1 {
				t.Fatalf("finalizer calls=%d", finalizerCalls)
			}
			var rows, events int
			if queryErr := db.DB.QueryRow("SELECT COUNT(*) FROM records WHERE (id=1 AND name='authorized') OR (id=2 AND name='unrelated')").Scan(&rows); queryErr != nil {
				t.Fatal(queryErr)
			}
			if queryErr := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); queryErr != nil {
				t.Fatal(queryErr)
			}
			if err == nil || rows != 2 || events != 0 {
				t.Fatalf("parent retargeted buffered child DELETE: error=%v protectedRows=%d audit=%d", err, rows, events)
			}
			if mode == "caller" {
				if len(callerOutcome.Transactions) != 1 || callerOutcome.Transactions[0].State != xhandler.TransactionCallerPending || callerOutcome.CommitConfirmed() {
					t.Fatalf("caller transaction ownership changed: %+v", callerOutcome)
				}
				if _, e := callerTx.ExecContext(ctx, "INSERT INTO records VALUES(3,'caller still owns')"); e != nil {
					t.Fatalf("caller transaction closed: %v", e)
				}
				if e := callerTx.Rollback(); e != nil {
					t.Fatal(e)
				}
				if e := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); e != nil || rows != 2 {
					t.Fatalf("caller rollback rows=%d error=%v", rows, e)
				}
			}

		})
	}
}

// The later unit mutates after both journals prepared; all-unit preflight must
// reject before the earlier owned transaction commits.
type policyPrepareData struct {
	*dml.Data
	after func(context.Context) error
}

func (d *policyPrepareData) PrepareCompletion(ctx context.Context) error {
	if err := d.Data.PrepareCompletion(ctx); err != nil {
		return err
	}
	return d.after(ctx)
}

type policyPrepareSource struct {
	data *policyPrepareData
	db   *sql.DB
}

func (s *policyPrepareSource) Open(context.Context) (xhandler.Data, error) { return s.data, nil }
func (s *policyPrepareSource) InvocationKey() any                          { return s.db }

func TestInsertDeleteAllUnitsRecheckBeforeFirstCommit(t *testing.T) {
	ctx := context.Background()
	first, second := sqlite.New(t), sqlite.New(t)
	for _, db := range []*sql.DB{first.DB, second.DB} {
		for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO records VALUES(1,'authorized'),(2,'unrelated')", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES(OLD.id);END"} {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "MultipleUnits"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/multiple-units"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/multiple-units"})
	if !ok {
		t.Fatal("route absent")
	}
	native, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "MultipleUnitsParent"}, Routes: []*spec.Route{{Method: "POST", Path: "/units-parent"}}}
	parentCompiled, err := compiler.New(compiler.Input{Component: parent, InputType: reflect.TypeFor[struct{}]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentRoute, ok := parentCompiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/units-parent"})
	if !ok {
		t.Fatal("parent route absent")
	}
	firstData := dml.NewData(first.DB)
	secondData := dml.NewData(second.DB)
	var later *policyReplayRow
	prepared := false
	wrapper := &policyPrepareData{Data: secondData, after: func(ctx context.Context) error {
		for _, data := range []*dml.Data{firstData, secondData} {
			_, tx := data.InvocationTransaction()
			if tx == nil {
				return fmt.Errorf("journal was not prepared")
			}
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 1 {
				return fmt.Errorf("prepared audit=%d err=%v", count, err)
			}
		}
		prepared = true
		*later.ID = 2
		return nil
	}}
	rootSource := &policyPrepareSource{data: &policyPrepareData{Data: firstData, after: func(context.Context) error { return nil }}, db: first.DB}
	secondSource := &policyPrepareSource{data: wrapper, db: second.DB}
	_, err = engine.New().Execute(ctx, engine.Request{Input: parentRoute, BoundInput: &struct{}{}, DataSource: rootSource, Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		for index, source := range []*policyPrepareSource{rootSource, secondSource} {
			id := 1
			input := &policyReplayInput{Rows: []*policyReplayRow{{ID: &id, Remove: true, Has: &policyReplayHas{ID: true, Remove: true}}}, CurrentRows: []*policyReplayRow{{ID: &id}}}
			result, e := engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: input, Handler: native, DataSource: source})
			if e != nil {
				return nil, e
			}
			if index == 1 {
				later = result.(*policyReplayOutput).Data[0]
			}
		}
		return "parent", nil
	})})
	if !prepared || err == nil {
		t.Fatalf("prepared=%v error=%v", prepared, err)
	}
	for index, db := range []*sql.DB{first.DB, second.DB} {
		var rows, events int
		if e := db.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); e != nil {
			t.Fatal(e)
		}
		if e := db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); e != nil {
			t.Fatal(e)
		}
		if rows != 2 || events != 0 {
			t.Fatalf("unit%d committed before later guard: rows=%d audit=%d", index, rows, events)
		}
	}
}

// The generic parent's error-aware finalizer runs after preparation and before
// root completion; it cannot invalidate an earlier native child journal.
type policyParentFinalizer struct{ finalize func(error) error }

func (o *policyParentFinalizer) Finalize(_ context.Context, cause error) error {
	return o.finalize(cause)
}

type policyObservedWriter struct {
	*writer.Handler
	observer xhandler.PhaseObserver
}

func (h *policyObservedWriter) NewPhaseObserver() xhandler.PhaseObserver { return h.observer }

type policyInvocationEndObserver struct{ ended func() }

func (o *policyInvocationEndObserver) ObservePhase(_ context.Context, event xhandler.PhaseEvent) {
	if event.Phase == xhandler.PhaseInvocation && event.Boundary == xhandler.PhaseEnd {
		o.ended()
	}
}
