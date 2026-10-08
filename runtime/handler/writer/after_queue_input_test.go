package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type aqiKey struct{}
type aqiProbe struct {
	calls  []string
	mode   string
	cancel context.CancelFunc
}
type aqiOutput struct {
	Data   []*sqPlainParent `parameter:"Data,kind=output,in=body"`
	Status string
	Note   string
}
type aqiRootHooks struct{}
type aqiChildHooks struct{}

var aqiLinked = []reflect.Type{reflect.TypeFor[aqiRootHooks](), reflect.TypeFor[aqiChildHooks]()}
var aqiError = errors.New("generic batch failure")

type aqiAllocationFailureBinder struct{ *sqBinder }

func (b *aqiAllocationFailureBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.SequencerKey {
		return b, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}
func (*aqiAllocationFailureBinder) Allocate(context.Context, string, any, string) error {
	return aqiError
}

func (*aqiRootHooks) Init(context.Context, *sqPlainParent, xhandler.LifecycleContext[sqPlainParent, xhandler.NoParent, aqiOutput]) error {
	return nil
}
func (*aqiRootHooks) Validate(ctx context.Context, _ *sqPlainParent, _ xhandler.LifecycleContext[sqPlainParent, xhandler.NoParent, aqiOutput]) error {
	if ctx.Value(aqiKey{}).(*aqiProbe).mode == "validation-error" {
		return aqiError
	}
	return nil
}
func (*aqiRootHooks) AfterQueue(ctx context.Context, row *sqPlainParent, state xhandler.LifecycleContext[sqPlainParent, xhandler.NoParent, aqiOutput]) error {
	p := ctx.Value(aqiKey{}).(*aqiProbe)
	p.calls = append(p.calls, fmt.Sprintf("root:%d", *row.ID))
	// This deliberate response alias must not permit queued body mutation.
	state.Output.Data = append(state.Output.Data, row)
	if p.mode == "row-error" {
		return aqiError
	}
	return nil
}
func (*aqiChildHooks) AfterQueue(ctx context.Context, row *sqPlainChild, _ xhandler.LifecycleContext[sqPlainChild, sqPlainParent, aqiOutput]) error {
	p := ctx.Value(aqiKey{}).(*aqiProbe)
	p.calls = append(p.calls, fmt.Sprintf("child:%d", *row.ID))
	if p.mode == "child-error" {
		return aqiError
	}
	return nil
}
func (*aqiRootHooks) AfterQueueInput(ctx context.Context, input *sqPlainInput, output *aqiOutput) error {
	p := ctx.Value(aqiKey{}).(*aqiProbe)
	p.calls = append(p.calls, "batch")
	output.Note = "independent response metadata"
	switch p.mode {
	case "batch-error":
		return aqiError
	case "panic":
		panic("generic batch panic")
	case "cancel":
		p.cancel()
	case "value":
		*input.Rows[0].Name = "illegal"
	case "output-alias":
		*output.Data[0].Name = "illegal alias"
	case "presence":
		input.Rows[0].Has.Name = false
	case "identity":
		*input.Rows[0].ID = 999
	case "association":
		input.Rows[0].Children = nil
	case "current":
		*input.CurrentRows[0].Name = "illegal current"
	}
	return nil
}

func aqiFixture(t *testing.T, mode string, noop, empty bool) (*Handler, rhandler.Invocation, *sqldml.Data, *sqlite.Harness, context.Context, *aqiProbe) {
	t.Helper()
	db := sqlite.New(t)
	for _, statement := range []string{
		"CREATE TABLE parents(id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id), label TEXT)",
	} {
		if _, err := db.DB.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	input := &sqPlainInput{}
	if !empty {
		for i := 1; i <= 2; i++ {
			id, childID, name, label := i, i+10, fmt.Sprintf("root %d", i), fmt.Sprintf("child %d", i)
			row := &sqPlainParent{ID: &id, Name: &name, Has: &sqPlainParentHas{ID: true, Name: true, Children: true}}
			if !noop {
				row.Children = []*sqPlainChild{{ID: &childID, Label: &label, Has: &sqPlainChildHas{ID: true, Label: true}}}
			}
			if noop {
				oldName := "stored " + name
				if _, err := db.DB.Exec("INSERT INTO parents(id,name) VALUES(?,?)", id, oldName); err != nil {
					t.Fatal(err)
				}
				input.CurrentRows = append(input.CurrentRows, &sqPlainParent{ID: &id, Name: &oldName})
				if noop {
					row.Has.Name, row.Has.Children = false, false
				}
			}
			input.Rows = append(input.Rows, row)
		}
	}
	data := sqldml.NewData(db.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	component := sqComponent(aqiLinked[0].PkgPath(), "patch", aqiLinked[0].Name())
	if mode == "allocation-error" {
		component.RootView.Columns[0].AutoIncrement = true
	}
	component.RootView.Relations[0].View.EntityHooks = aqiLinked[1].Name()
	h, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[aqiOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	probe := &aqiProbe{mode: mode}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), aqiKey{}, probe))
	probe.cancel = cancel
	t.Cleanup(cancel)
	var binder xhandler.Binder = &sqBinder{data: data}
	if mode == "allocation-error" {
		binder = &aqiAllocationFailureBinder{binder.(*sqBinder)}
	}
	return h, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: binder}, data, db, ctx, probe
}

func TestAfterQueueInputOrderingEmptyAndNoop(t *testing.T) {
	for _, mode := range []string{"rows", "empty", "noop"} {
		t.Run(mode, func(t *testing.T) {
			h, invocation, data, db, ctx, probe := aqiFixture(t, mode, mode == "noop", mode == "empty")
			result, err := h.Execute(ctx, invocation)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"batch"}
			if mode == "rows" {
				want = []string{"root:1", "child:11", "root:2", "child:12", "batch"}
			}
			if !reflect.DeepEqual(probe.calls, want) {
				t.Fatalf("queue trace %v, want %v", probe.calls, want)
			}
			if result.(*aqiOutput).Note != "independent response metadata" {
				t.Fatal("metadata update lost")
			}
			if err := data.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM parents").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if mode == "empty" && count != 0 || mode != "empty" && count != 2 {
				t.Fatalf("persisted roots %d", count)
			}
		})
	}
}

func TestAfterQueueInputFailurePrefixAndQueuedImmutability(t *testing.T) {
	for _, mode := range []string{"validation-error", "allocation-error", "row-error", "child-error", "batch-error", "cancel", "panic", "value", "output-alias", "presence", "identity", "association", "current"} {
		t.Run(mode, func(t *testing.T) {
			h, invocation, data, db, ctx, probe := aqiFixture(t, mode, mode == "current", false)
			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, err = h.Execute(ctx, invocation)
			}()
			if mode == "panic" {
				if recovered == nil {
					t.Fatal("panic did not propagate")
				}
			} else if err == nil {
				t.Fatal("failed execution accepted")
			}
			if mode == "validation-error" || mode == "allocation-error" || mode == "row-error" || mode == "child-error" {
				if strings.Contains(strings.Join(probe.calls, ","), "batch") {
					t.Fatal("batch dispatched after earlier failure")
				}
			} else if probe.calls[len(probe.calls)-1] != "batch" {
				t.Fatal("missing batch callback")
			}
			// Even swallowing the returned error/panic cannot turn the retained
			// owner into success or flush the queued prefix.
			if err := data.Complete(context.Background(), nil); err == nil {
				t.Fatal("failed owner completed successfully")
			}
			var count int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM parents").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "current" {
				want = 2
			}
			if count != want {
				t.Fatalf("failed prefix persisted %d rows, want %d", count, want)
			}
		})
	}
}

func TestAfterQueueInputRetainedGraphAndEmptyFailure(t *testing.T) {
	for _, mode := range []string{"late body alias", "late Previous", "late Original", "late PreviousFields", "late lifecycle PreviousFields", "empty error", "noop error", "cancel before entry"} {
		t.Run(mode, func(t *testing.T) {
			noop := mode == "noop error" || mode == "late Previous" || mode == "late PreviousFields" || mode == "late lifecycle PreviousFields"
			empty := mode == "empty error"
			hookMode := ""
			if mode == "empty error" || mode == "noop error" {
				hookMode = "batch-error"
			}
			h, invocation, data, db, ctx, probe := aqiFixture(t, hookMode, noop, empty)
			if mode == "cancel before entry" {
				probe.cancel()
			}
			_, err := h.Execute(ctx, invocation)
			if hookMode != "" || mode == "cancel before entry" {
				if err == nil {
					t.Fatal("boundary failure accepted")
				}
				if mode == "cancel before entry" && strings.Contains(strings.Join(probe.calls, ","), "batch") {
					t.Fatal("cancelled boundary entered")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				p := invocation.Snapshot.(*Program)
				switch mode {
				case "late body alias":
					*invocation.Input.(*sqPlainInput).Rows[0].Name = "late illegal"
				case "late Previous":
					*p.frames.Rows[0].Previous.Interface().(*sqPlainParent).Name = "late illegal previous"
				case "late Original":
					p.frames.Rows[0].Original = originalPresence{available: false}
				case "late PreviousFields":
					if p.previousFields == nil {
						p.previousFields = map[*Record]fieldSet{}
					}
					p.previousFields[p.metadata.Root] = fieldSet{"illegal": true}
				case "late lifecycle PreviousFields":
					delete(p.typeFields[reflect.TypeFor[sqPlainParent]()], "Name")
				}
			}
			if err := data.Complete(context.Background(), nil); err == nil {
				t.Fatal("retained failure completed")
			}
			var count int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM parents").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if noop {
				want = 2
			}
			if count != want {
				t.Fatalf("count=%d want=%d", count, want)
			}
		})
	}
}

func TestAfterQueueInputNoReplayAfterEntry(t *testing.T) {
	h, invocation, _, _, ctx, probe := aqiFixture(t, "batch-error", false, false)
	if _, err := h.Execute(ctx, invocation); !errors.Is(err, aqiError) {
		t.Fatal(err)
	}
	if _, err := h.Execute(ctx, invocation); err == nil {
		t.Fatal("same captured attempt replayed")
	}
	if n := strings.Count(strings.Join(probe.calls, ","), "batch"); n != 1 {
		t.Fatalf("callback calls %d", n)
	}
	h.transactionRetrySupported, h.recoverySupported, h.scopedSequences = true, true, true
	if allowed, err := h.RetryTransaction(ctx, invocation, nil, rhandler.MutationOutcome{}); err != nil || allowed {
		t.Fatalf("transaction replay allowed=%v err=%v", allowed, err)
	}
	if decision, err := h.RecoverMutation(ctx, invocation, nil, rhandler.MutationOutcome{}); err != nil || decision != rhandler.RecoveryNone {
		t.Fatalf("recovery=%v err=%v", decision, err)
	}
	if allowed, err := h.RecoverScopedMutation(ctx, invocation, exec.MutationReport{Contention: true}, xhandler.Outcome{Transactions: []xhandler.TransactionOutcome{{State: xhandler.TransactionRolledBack}}}); err != nil || allowed {
		t.Fatalf("scoped replay allowed=%v err=%v", allowed, err)
	}
}

type aqiWrongInputHooks struct{}

func (*aqiWrongInputHooks) AfterQueueInput(context.Context, *hookInput, *aqiOutput) error { return nil }

type aqiVariadicHooks struct{}

func (*aqiVariadicHooks) AfterQueueInput(context.Context, *sqPlainInput, ...*aqiOutput) error {
	return nil
}

func TestAfterQueueInputRuntimeCanonicalAndRootContracts(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[aqiWrongInputHooks](), reflect.TypeFor[aqiVariadicHooks]()} {
		root := &Record{HookType: typ}
		if err := validateAfterQueueInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[aqiOutput]()); err == nil {
			t.Fatal("invalid signature accepted", typ)
		}
	}
	root := &Record{Relations: []*Relation{{Child: &Record{HookType: reflect.TypeFor[aqiRootHooks]()}}}}
	if err := validateAfterQueueInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[aqiOutput]()); err == nil {
		t.Fatal("descendant batch hook accepted")
	}
}
