package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	engine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// All persistence is through the real native writer. The SQL trigger is an
// actual physical-write witness, not an activity/owner counter.
func activity49DB(t *testing.T) *sqlite.Harness {
	t.Helper()
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(),
		`CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)`,
		`CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))`,
		`CREATE TABLE journal_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)`,
		`CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal_order(id) VALUES(new.id); END`); err != nil {
		t.Fatal(err)
	}
	return db
}
func activity49Native(t *testing.T, name, path, policy string, db *sqlite.Harness) *registry.RegisteredComponent {
	t.Helper()
	s := componentSpec(name, "PATCH", path, nil)
	s.Settings = &spec.Settings{Mutation: "patch", ComponentCallPolicy: policy}
	s.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: s, InputType: reflect.TypeFor[bufferedCallInput](), OutputType: reflect.TypeFor[bufferedCallOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	h, err := writer.New(a.Component, reflect.TypeFor[bufferedCallInput](), reflect.TypeFor[bufferedCallOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	v, err := a.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: h, Providers: []locator.Provider{v}, DataSource: dml.Source{DB: db.DB}}
}
func activity49Parent(t *testing.T, name, path, policy string, db *sqlite.Harness, handler rh.Handler) *registry.RegisteredComponent {
	t.Helper()
	s := componentSpec(name, "POST", path, nil)
	s.Settings = &spec.Settings{ComponentCallPolicy: policy}
	a := componentArtifact(t, s, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
	r := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: handler}
	if db != nil {
		r.DataSource = dml.Source{DB: db.DB}
	}
	return r
}
func activity49Request(r *registry.RegisteredComponent, id int) dexec.ComponentRequest {
	req := dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: r.Component.Key, Route: spec.RouteRef{Method: r.Component.Routes[0].Method, Path: r.Component.Routes[0].Path}}, Input: &struct{}{}}
	if r.Component.Routes[0].Method == "PATCH" {
		req.Input = &bufferedCallInput{Rows: []*bufferedCallRow{{ID: id, Label: fmt.Sprintf("native-%d", id)}}}
	}
	return req
}
func activity49Invoker(ctx context.Context, b xh.Binder) (dexec.ComponentInvoker, error) {
	v, ok, err := b.Lookup(ctx, dexec.ComponentInvokerKey)
	if err != nil || !ok {
		return nil, fmt.Errorf("real invoker lookup found=%t: %w", ok, err)
	}
	return v.(dexec.ComponentInvoker), nil
}
func activity49Rows(ctx context.Context, db *sql.DB) ([]int, error) {
	rows, err := db.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
func activity49Pending(ctx context.Context, db *sql.DB) (int, error) {
	tx, err := dexec.InvocationTransaction(ctx, db)
	if err != nil {
		return 0, err
	}
	var n int
	if tx != nil {
		err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&n)
	} else {
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&n)
	}
	return n, err
}
func activity49Runtime(t *testing.T, entries ...*registry.RegisteredComponent) *Runtime {
	t.Helper()
	r, err := NewRuntime(entries)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown(context.Background()) })
	return r
}

func TestNativeActivity49OrdinarySourceLessOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("parent-error=%t", fail), func(t *testing.T) {
			db := activity49DB(t)
			child := activity49Native(t, "OrdinaryChild", "/ordinary-child", "", db)
			sentinel := errors.New("later source-less parent failure")
			parent := activity49Parent(t, "OrdinaryParent", "/ordinary-parent", "", nil, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("ordinary parent policy changed")
				}
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				out, err := caller.InvokeComponent(context.Background(), activity49Request(child, 1))
				if err != nil {
					return nil, err
				}
				rows, err := activity49Rows(ctx, db.DB)
				if err != nil || !reflect.DeepEqual(rows, []int{1}) {
					return nil, fmt.Errorf("native child not committed before parent return rows=%v err=%v", rows, err)
				}
				t.Logf("REAL_CHILD_COMMIT_BEFORE_PARENT_RETURN=%v", rows)
				if fail {
					return nil, sentinel
				}
				return out, nil
			}))
			r := activity49Runtime(t, parent, child)
			_, err := r.InvokeComponent(context.Background(), activity49Request(parent, 0))
			if fail {
				if !errors.Is(err, sentinel) {
					t.Fatalf("lost later parent cause: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			rows, e := activity49Rows(context.Background(), db.DB)
			if e != nil || !reflect.DeepEqual(rows, []int{1}) {
				t.Fatalf("source-less parent retroactively rolled back child rows=%v err=%v", rows, e)
			}
		})
	}
}

func TestNativeActivity49BufferedNeutralRootOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("parent-error=%t", fail), func(t *testing.T) {
			db := activity49DB(t)
			child := activity49Native(t, "NeutralChild", "/neutral-child", "", db)
			sentinel := errors.New("buffered neutral parent failure")
			var retained dexec.ComponentInvoker
			parent := activity49Parent(t, "NeutralParent", "/neutral-parent", "buffered", nil, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if !engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("buffered neutral policy absent")
				}
				var err error
				retained, err = activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				out, err := retained.InvokeComponent(context.Background(), activity49Request(child, 2))
				if err != nil {
					return nil, err
				}
				n, err := activity49Pending(ctx, db.DB)
				if err != nil || n != 0 {
					return nil, fmt.Errorf("native child drained before neutral parent n=%d err=%v", n, err)
				}
				t.Log("REAL_BUFFERED_CHILD_PENDING_SQL=0")
				if fail {
					return nil, sentinel
				}
				return out, nil
			}))
			r := activity49Runtime(t, parent, child)
			_, err := r.InvokeComponent(context.Background(), activity49Request(parent, 0))
			if fail {
				if !errors.Is(err, sentinel) {
					t.Fatalf("lost rollback cause %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			rows, e := activity49Rows(context.Background(), db.DB)
			want := []int{2}
			if fail {
				want = nil
			}
			if e != nil || !reflect.DeepEqual(rows, want) {
				t.Fatalf("neutral physical outcome rows=%v want=%v err=%v", rows, want, e)
			}
			if _, lateErr := retained.InvokeComponent(context.Background(), activity49Request(child, 9)); lateErr == nil {
				t.Fatal("closed protected root admitted retained native child")
			}
			rows, e = activity49Rows(context.Background(), db.DB)
			if e != nil || !reflect.DeepEqual(rows, want) {
				t.Fatalf("closed root late mutation rows=%v err=%v", rows, e)
			}
		})
	}
}

func TestNativeActivity49BufferedSubtreeChildOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("outer-error=%t", fail), func(t *testing.T) {
			db := activity49DB(t)
			native := activity49Native(t, "SubtreeNative", "/subtree-native", "", db)
			sentinel := errors.New("outer ordinary parent failure")
			child := activity49Parent(t, "BufferedSubtree", "/buffered-subtree", "buffered", nil, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				out, err := caller.InvokeComponent(context.Background(), activity49Request(native, 3))
				if err != nil {
					return nil, err
				}
				n, err := activity49Pending(ctx, db.DB)
				if err != nil || n != 0 {
					return nil, fmt.Errorf("subtree native SQL ran before composing child end n=%d err=%v", n, err)
				}
				return out, nil
			}))
			outer := activity49Parent(t, "OrdinaryOuter", "/ordinary-outer", "", nil, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("source-less ordinary outer promoted")
				}
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				out, err := caller.InvokeComponent(context.Background(), activity49Request(child, 0))
				if err != nil {
					return nil, err
				}
				rows, err := activity49Rows(ctx, db.DB)
				if err != nil || !reflect.DeepEqual(rows, []int{3}) {
					return nil, fmt.Errorf("composing subtree did not complete before ordinary outer rows=%v err=%v", rows, err)
				}
				t.Logf("REAL_BUFFERED_SUBTREE_COMMITTED_BEFORE_OUTER=%v", rows)
				if fail {
					return nil, sentinel
				}
				return out, nil
			}))
			r := activity49Runtime(t, outer, child, native)
			_, err := r.InvokeComponent(context.Background(), activity49Request(outer, 0))
			if fail {
				if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			rows, e := activity49Rows(context.Background(), db.DB)
			if e != nil || !reflect.DeepEqual(rows, []int{3}) {
				t.Fatalf("outer stole subtree ownership rows=%v err=%v", rows, e)
			}
		})
	}
}

// Existing public custom orchestration contract, with genuine native descendants.
func TestNativeActivity49IndependentOrdinaryPolicy(t *testing.T) {
	db := activity49DB(t)
	native := activity49Native(t, "IndependentNative", "/independent-native", "", db)
	sentinel := errors.New("independent ordinary outer failure")
	var retained dexec.ComponentInvoker
	contract := custom.New[struct{}, bufferedCallOutput](xh.ContractFunc[struct{}, bufferedCallOutput](func(ctx context.Context, s xh.Session, _ *struct{}, out *bufferedCallOutput) error {
		if engine.IsBufferedComponent(ctx) {
			return fmt.Errorf("independent ordinary caller became buffered")
		}
		var err error
		retained, err = activity49Invoker(ctx, s.Binder())
		if err != nil {
			return err
		}
		value, err := retained.InvokeComponent(context.Background(), activity49Request(native, 4))
		if err != nil {
			return err
		}
		*out = *value.(*bufferedCallOutput)
		rows, err := activity49Rows(ctx, db.DB)
		if err != nil || !reflect.DeepEqual(rows, []int{4}) {
			return fmt.Errorf("independent native did not commit before custom parent rows=%v err=%v", rows, err)
		}
		return sentinel
	}))
	parent := activity49Parent(t, "IndependentParent", "/independent-parent", "", nil, contract)
	r := activity49Runtime(t, parent, native)
	request := activity49Request(parent, 0)
	request.IndependentChildTransactions = true
	_, err := r.InvokeComponent(context.Background(), request)
	if !errors.Is(err, sentinel) {
		t.Fatalf("independent contract cause %v", err)
	}
	if _, err = retained.InvokeComponent(context.Background(), activity49Request(native, 5)); err != nil {
		t.Fatalf("ordinary retained/replaced context policy escaped eligibility: %v", err)
	}
	if _, err = r.InvokeComponent(context.Background(), activity49Request(native, 6)); err != nil {
		t.Fatalf("independent ordinary fresh invocation failed %v", err)
	}
	rows, e := activity49Rows(context.Background(), db.DB)
	if e != nil || !reflect.DeepEqual(rows, []int{4, 5, 6}) {
		t.Fatalf("ordinary independent physical state %v %v", rows, e)
	}
	t.Logf("REAL_ORDINARY_INDEPENDENT_AND_RETAINED_COMMITS=%v", rows)
}

// The wrapper embeds the complete native handler ABI; only the real return
// boundary is held. This is actual executing native work, not a fabricated token.
type activity49ReturnGate struct {
	*writer.Handler
	entered chan struct{}
	release chan struct{}
}

func (h *activity49ReturnGate) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	out, err := h.Handler.Execute(ctx, inv)
	if err != nil {
		return out, err
	}
	close(h.entered)
	select {
	case <-h.release:
	case <-ctx.Done():
		return out, ctx.Err()
	}
	return out, nil
}

// Desired fail-closed activity barrier. Keep this baseline red if native root
// completion succeeds while its genuine admitted sibling is still returning.
func TestNativeActivity49DesiredLateEnrollmentRejectsLiveWork(t *testing.T) {
	for _, newUnit := range []bool{false, true} {
		t.Run(fmt.Sprintf("new-db-unit=%t", newUnit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db := activity49DB(t)
			other := db
			if newUnit {
				other = activity49DB(t)
			}
			sibling := activity49Native(t, "LiveSibling", "/live-sibling", "", other)
			gate := &activity49ReturnGate{Handler: sibling.Handler.(*writer.Handler), entered: make(chan struct{}), release: make(chan struct{})}
			sibling.Handler = gate
			buffered := activity49Native(t, "LateBuffered", "/late-buffered", "buffered", db)
			siblingDone := make(chan error, 1)
			parent := activity49Parent(t, "ExistingOrdinaryRoot", "/existing-ordinary-root", "", db, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("ordinary root policy promoted")
				}
				value, ok, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !ok {
					return nil, fmt.Errorf("root data %v", err)
				}
				if err = value.(xh.Data).Execute("INSERT INTO records VALUES(1,'ordinary prefix')"); err != nil {
					return nil, err
				}
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				go func() { _, err := caller.InvokeComponent(ctx, activity49Request(sibling, 3)); siblingDone <- err }()
				select {
				case <-gate.entered:
				case err := <-siblingDone:
					return nil, fmt.Errorf("sibling failed before actual native return gate: %w", err)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				n, err := activity49Pending(ctx, other.DB)
				if err != nil {
					return nil, err
				}
				t.Logf("REAL_ORDINARY_SIBLING_STILL_EXECUTING_PREFIX=%d newUnit=%t", n, newUnit)
				out, err := caller.InvokeComponent(ctx, activity49Request(buffered, 2))
				if err != nil {
					return nil, err
				}
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("late enrollment stamped ordinary root buffered")
				}
				return out, nil
			}))
			r := activity49Runtime(t, parent, sibling, buffered)
			_, rootErr := r.InvokeComponent(ctx, activity49Request(parent, 0))
			rootRows, e := activity49Rows(context.Background(), db.DB)
			if e != nil {
				t.Error(e)
			}
			var otherRows []int
			if newUnit {
				otherRows, e = activity49Rows(context.Background(), other.DB)
				if e != nil {
					t.Error(e)
				}
			}
			close(gate.release)
			select {
			case err := <-siblingDone:
				t.Logf("REAL_SIBLING_RETURN_AFTER_ROOT err=%v", err)
			case <-ctx.Done():
				t.Error("sibling cleanup timed out")
			}
			t.Logf("REAL_ROOT_COMPLETION_WITH_LIVE_NATIVE rootErr=%v rows=%v otherRows=%v", rootErr, rootRows, otherRows)
			if rootErr == nil {
				t.Error("DESIRED_GAP: canonical root committed before live admitted native activity finished")
			}
			if len(rootRows) != 0 || len(otherRows) != 0 {
				t.Errorf("DESIRED_GAP: unfinished native work permitted physical commit root=%v other=%v", rootRows, otherRows)
			}
		})
	}
}

// The ordinary composing sibling has no invented outcome/activity API. Its
// real native descendant finishes, while this parent is still inside Execute.
// Outcome-frame completion of the descendant is not completion of its parent.
func TestNativeActivity49DesiredLiveComposingParent(t *testing.T) {
	for _, newUnit := range []bool{false, true} {
		t.Run(fmt.Sprintf("new-db-unit=%t", newUnit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db := activity49DB(t)
			other := db
			if newUnit {
				other = activity49DB(t)
			}
			native := activity49Native(t, "ComposingDescendant", "/composing-descendant", "", other)
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			lateCause := errors.New("ordinary composing parent fails after enrollment")
			sibling := activity49Parent(t, "LiveComposingSibling", "/live-composing-sibling", "", other, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("ordinary composing sibling stamped buffered")
				}
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				out, err := caller.InvokeComponent(ctx, activity49Request(native, 3))
				if err != nil {
					return nil, err
				}
				if out.(*bufferedCallOutput).Data[0].ID != 3 {
					return nil, fmt.Errorf("native physical/public identity changed")
				}
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return out, ctx.Err()
				}
				return out, lateCause
			}))
			buffered := activity49Native(t, "ComposingLateBuffered", "/composing-late-buffered", "buffered", db)
			parent := activity49Parent(t, "ExistingComposingRoot", "/existing-composing-root", "", db, rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, ok, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !ok {
					return nil, fmt.Errorf("root actual data %v", err)
				}
				if err = value.(xh.Data).Execute("INSERT INTO records VALUES(1,'root prefix')"); err != nil {
					return nil, err
				}
				caller, err := activity49Invoker(ctx, inv.Binder)
				if err != nil {
					return nil, err
				}
				go func() { _, err := caller.InvokeComponent(ctx, activity49Request(sibling, 0)); done <- err }()
				select {
				case <-entered:
				case err := <-done:
					return nil, fmt.Errorf("sibling failed before completed native descendant %w", err)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				n, err := activity49Pending(ctx, other.DB)
				if err != nil {
					return nil, err
				}
				t.Logf("REAL_COMPOSING_PARENT_LIVE_NATIVE_DESCENDANT_FINISHED_PREFIX=%d newUnit=%t", n, newUnit)
				out, err := caller.InvokeComponent(ctx, activity49Request(buffered, 2))
				if err != nil {
					return nil, err
				}
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("late protected ordinary root policy promoted")
				}
				return out, nil
			}))
			r := activity49Runtime(t, parent, sibling, native, buffered)
			_, rootErr := r.InvokeComponent(ctx, activity49Request(parent, 0))
			rootRows, e := activity49Rows(context.Background(), db.DB)
			if e != nil {
				t.Error(e)
			}
			var otherRows []int
			if newUnit {
				otherRows, e = activity49Rows(context.Background(), other.DB)
				if e != nil {
					t.Error(e)
				}
			}
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, lateCause) {
					t.Errorf("late actual composing error identity lost %v", err)
				}
				t.Logf("REAL_COMPOSING_PARENT_FINISH_AFTER_ROOT=%v", err)
			case <-ctx.Done():
				t.Error("composing sibling cleanup timed out")
			}
			t.Logf("REAL_ENROLLED_ROOT_WITH_LIVE_COMPOSING_PARENT rootErr=%v rows=%v otherRows=%v", rootErr, rootRows, otherRows)
			if rootErr == nil {
				t.Error("DESIRED_ACTIVITY_GAP: root completed successfully while actual admitted composing parent remained live")
			}
			if len(rootRows) != 0 || len(otherRows) != 0 {
				t.Errorf("DESIRED_ACTIVITY_GAP: live composing parent allowed physical commit root=%v other=%v", rootRows, otherRows)
			}
		})
	}
}
