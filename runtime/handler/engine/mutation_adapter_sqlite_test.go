package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	adapter "github.com/viant/datly/runtime/handler/mutation"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	policy "github.com/viant/xdatly/handler/mutation"
)

type mutationPhaseInput struct {
	ID     int
	HasID  bool
	Events *[]string
	Fail   string
}

func (i *mutationPhaseInput) Init(context.Context) error {
	*i.Events = append(*i.Events, "input init")
	i.ID = 40
	if i.Fail == "input init" {
		return errors.New(i.Fail)
	}
	return nil
}
func (i *mutationPhaseInput) InitMCP(context.Context, xmcp.Context) error {
	*i.Events = append(*i.Events, "input MCP")
	if i.Fail == "input MCP" {
		return errors.New(i.Fail)
	}
	return nil
}

type mutationPhaseOutput struct{ ID int }
type mutationPhaseDefinition struct {
	data      *sqldml.Data
	writes    bool
	report    *xhandler.Outcome
	finalized *int
	cancelAt  string
	cancel    context.CancelFunc
}

func (d *mutationPhaseDefinition) Capture(_ context.Context, input *mutationPhaseInput) (policy.Program[mutationPhaseOutput], error) {
	*input.Events = append(*input.Events, "capture")
	if input.Fail == "capture" {
		return nil, errors.New("capture")
	}
	program := &mutationPhaseProgram{definition: d, input: input, originalID: input.ID, originalHas: input.HasID}
	if input.Fail == "partial capture" {
		return program, errors.New(input.Fail)
	}
	if input.Fail == "nil capture" {
		return (*mutationPhaseProgram)(nil), nil
	}
	return program, nil
}
func (d *mutationPhaseDefinition) FinalizeFailure(_ context.Context, input *mutationPhaseInput, _ *mutationPhaseOutput, outcome xhandler.Outcome) error {
	*input.Events = append(*input.Events, "failure finalizer")
	*d.finalized++
	*d.report = outcome
	return nil
}

type mutationPhaseProgram struct {
	definition  *mutationPhaseDefinition
	input       *mutationPhaseInput
	originalID  int
	originalHas bool
	dml         xhandler.DML
	output      mutationPhaseOutput
}

func (p *mutationPhaseProgram) phase(name string) error {
	*p.input.Events = append(*p.input.Events, name)
	if p.definition.cancelAt == name && p.definition.cancel != nil {
		p.definition.cancel()
	}
	if p.input.Fail == name {
		return errors.New(name)
	}
	return nil
}
func (p *mutationPhaseProgram) Prepare(ctx context.Context, binder xhandler.Binder) error {
	if err := p.phase("prepare"); err != nil {
		return err
	}
	if !p.definition.writes {
		return nil
	}
	value, found, err := binder.Lookup(ctx, xhandler.DMLKey)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("DML missing")
	}
	p.dml = value.(xhandler.DML)
	return nil
}
func (p *mutationPhaseProgram) SyncPresence(context.Context) error {
	if p.originalID != 0 || p.originalHas {
		return errors.New("original capture was mutated")
	}
	if p.input.ID != 40 {
		return errors.New("input Init did not run before sync")
	}
	p.input.HasID = true
	return p.phase("sync")
}
func (p *mutationPhaseProgram) Invariants(context.Context) error {
	if !p.input.HasID {
		return errors.New("invariants precede presence")
	}
	p.input.ID++
	return p.phase("invariants")
}
func (p *mutationPhaseProgram) Init(context.Context) error {
	if p.input.ID != 41 {
		return errors.New("entity Init precedes invariants")
	}
	p.input.ID++
	return p.phase("entity init")
}
func (p *mutationPhaseProgram) Validate(context.Context) error {
	if p.definition.data.TransactionOutcome().State != xhandler.TransactionNone {
		return errors.New("transaction started before validation")
	}
	return p.phase("validate")
}
func (p *mutationPhaseProgram) RequiresTransaction() bool { return p.definition.writes }
func (p *mutationPhaseProgram) Sequence(context.Context) error {
	if p.definition.writes && p.definition.data.TransactionOutcome().State != xhandler.TransactionUnknown {
		return errors.New("sequence precedes transaction Start")
	}
	return p.phase("sequence")
}
func (p *mutationPhaseProgram) AfterSequence(context.Context) error { return p.phase("after sequence") }
func (p *mutationPhaseProgram) Diff(context.Context) error          { return p.phase("diff") }
func (p *mutationPhaseProgram) Reconcile(context.Context) error {
	p.output.ID = p.input.ID
	return p.phase("reconcile")
}
func (p *mutationPhaseProgram) Queue(context.Context) error {
	if err := p.phase("queue"); err != nil {
		return err
	}
	if p.definition.writes {
		return p.dml.Execute("INSERT INTO audit(id) VALUES(?)", p.input.ID)
	}
	return nil
}
func (p *mutationPhaseProgram) AfterQueue(context.Context) error { return p.phase("after queue") }
func (p *mutationPhaseProgram) Output() *mutationPhaseOutput     { return &p.output }
func (p *mutationPhaseProgram) Finalize(_ context.Context, outcome xhandler.Outcome) error {
	*p.definition.finalized++
	*p.definition.report = outcome
	return p.phase("finalize")
}

func TestMutationAdapterPhaseOrderSQLite(t *testing.T) {
	stages := []string{"capture", "input init", "input MCP", "prepare", "sync", "invariants", "entity init", "validate", "sequence", "after sequence", "diff", "reconcile", "queue", "after queue", "finalize"}
	for _, fail := range append([]string{""}, stages...) {
		t.Run(fmt.Sprintf("failure=%s", fail), func(t *testing.T) {
			ctx := xmcp.WithContext(context.Background(), engineMCPContext{})
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(h.DB)
			events := []string{}
			calls := 0
			var report xhandler.Outcome
			definition := &mutationPhaseDefinition{data: data, writes: true, report: &report, finalized: &calls}
			input := &mutationPhaseInput{Events: &events, Fail: fail}
			result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(*input)), BoundInput: input, DataSource: &staticDataSource{data: data}, Handler: adapter.New[mutationPhaseInput, mutationPhaseOutput](definition)})
			if (err != nil) != (fail != "") || calls != 1 {
				t.Fatalf("error=%v finalizers=%d", err, calls)
			}
			want := append([]string(nil), stages...)
			if fail != "" {
				for index, name := range stages {
					if name == fail {
						want = append([]string(nil), stages[:index+1]...)
						break
					}
				}
				if fail == "capture" {
					want = append(want, "failure finalizer")
				} else if fail != "finalize" {
					want = append(want, "finalize")
				}
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("phases=%v want=%v", events, want)
			}
			committed := fail == "" || fail == "finalize"
			if report.CommitConfirmed() != committed {
				t.Fatalf("outcome=%+v", report)
			}
			count := 0
			if committed {
				count = 1
				if result.(*mutationPhaseOutput).ID != 42 {
					t.Fatalf("output=%+v", result)
				}
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{count}})
		})
	}
}

func TestMutationAdapterCancellationSQLite(t *testing.T) {
	for _, stage := range []string{"validate", "sequence", "after queue"} {
		t.Run(stage, func(t *testing.T) {
			base := context.Background()
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			h := sqlite.New(t)
			if err := h.ExecStatements(base, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			events := []string{}
			calls := 0
			var report xhandler.Outcome
			data := sqldml.NewData(h.DB)
			definition := &mutationPhaseDefinition{data: data, writes: true, report: &report, finalized: &calls, cancelAt: stage, cancel: cancel}
			input := &mutationPhaseInput{Events: &events}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(*input)), BoundInput: input, DataSource: &staticDataSource{data: data}, Handler: adapter.New[mutationPhaseInput, mutationPhaseOutput](definition)})
			if err == nil || calls != 1 || report.Error == nil || report.CommitConfirmed() {
				t.Fatalf("error=%v finalizers=%d outcome=%+v events=%v", err, calls, report, events)
			}
			if stage == "validate" && report.State() != xhandler.TransactionNone {
				t.Fatalf("cancelled validation started transaction: %+v", report)
			}
			h.AssertQuery(t, base, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
		})
	}
}

func TestMutationAdapterWithoutWritableRecordsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	events := []string{}
	calls := 0
	var report xhandler.Outcome
	definition := &mutationPhaseDefinition{data: sqldml.NewData(h.DB), report: &report, finalized: &calls}
	input := &mutationPhaseInput{Events: &events}
	result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(*input)), BoundInput: input, Handler: adapter.New[mutationPhaseInput, mutationPhaseOutput](definition)})
	if err != nil || calls != 1 || report.CommitConfirmed() || report.State() != xhandler.TransactionNone {
		t.Fatalf("result=%v error=%v outcome=%+v calls=%d", result, err, report, calls)
	}
	if result.(*mutationPhaseOutput).ID != 42 {
		t.Fatalf("output=%+v", result)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
}

func TestMutationAdapterCaptureFailureFinalizationSQLite(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []string
	}{
		{"capture", []string{"capture", "failure finalizer"}},
		{"partial capture", []string{"capture", "finalize"}},
		{"nil capture", []string{"capture", "failure finalizer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			events := []string{}
			calls := 0
			var report xhandler.Outcome
			definition := &mutationPhaseDefinition{data: sqldml.NewData(h.DB), report: &report, finalized: &calls}
			input := &mutationPhaseInput{Events: &events, Fail: test.name}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(*input)), BoundInput: input, Handler: adapter.New[mutationPhaseInput, mutationPhaseOutput](definition)})
			if err == nil || calls != 1 || report.CommitConfirmed() || report.Error == nil || !reflect.DeepEqual(events, test.events) {
				t.Fatalf("error=%v outcome=%+v calls=%d events=%v", err, report, calls, events)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
		})
	}
}
