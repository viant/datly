package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	sqlreader "github.com/viant/datly/sql/reader"
	h "github.com/viant/xdatly/handler"
)

type preparationProbeKey struct{}
type preparationProbe struct {
	mode                                           string
	trace                                          []string
	roots, childInit, childValidate, childSequence int
	discarded                                      *eligibleLeaf
	retained                                       *eligibleLeaf
	current                                        *eligibleParent
	cancel                                         context.CancelFunc
	reader, mutation                               dexec.ComponentTarget
	originalID, originalParentID                   bool
}

func preparationProbeFrom(ctx context.Context) *preparationProbe {
	return ctx.Value(preparationProbeKey{}).(*preparationProbe)
}

var preparationFailure = errors.New("preparation fixture error")

type preparationRootHook struct {
	Input     *eligibleParentInput   `bind:"kind=input,required"`
	Invoker   dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
	DML       h.DML                  `bind:"kind=dml,required"`
	Sequence  h.Sequencer            `bind:"kind=sequencer,required"`
	Flush     h.Flusher              `bind:"kind=flusher,required"`
	initCount int
}
type preparationChildHook struct{}

var preparationLinked = []reflect.Type{reflect.TypeFor[preparationRootHook](), reflect.TypeFor[preparationChildHook]()}

func (hook *preparationRootHook) Init(ctx context.Context, _ *eligibleParent, state h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	state.Output.Data = hook.Input.Rows
	hook.initCount++
	preparationProbeFrom(ctx).roots++
	return nil
}
func (*preparationRootHook) Validate(ctx context.Context, _ *eligibleParent, _ h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	p := preparationProbeFrom(ctx)
	p.trace = append(p.trace, "root validate")
	if p.mode == "business error" {
		return preparationFailure
	}
	return nil
}
func (*preparationChildHook) Init(ctx context.Context, _ *eligibleLeaf, _ h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	preparationProbeFrom(ctx).childInit++
	return nil
}
func (*preparationChildHook) Validate(ctx context.Context, _ *eligibleLeaf, _ h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	p := preparationProbeFrom(ctx)
	p.childValidate++
	p.trace = append(p.trace, "child validate")
	if p.mode == "cancel validation" {
		p.cancel()
	}
	return nil
}
func (*preparationChildHook) AfterSequence(ctx context.Context, row *eligibleLeaf, state h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	p := preparationProbeFrom(ctx)
	p.childSequence++
	if row == p.retained {
		p.originalID = state.Original.Has("ID")
		p.originalParentID = state.Original.Has("ParentID")
	}
	return nil
}
func (hook *preparationRootHook) AfterValidateInput(ctx context.Context, input *eligibleParentInput, output *eligibleParentOutput) error {
	p := preparationProbeFrom(ctx)
	p.trace = append(p.trace, "prepare")
	if hook.initCount != p.roots {
		return fmt.Errorf("lifecycle instance changed")
	}
	// The body alias was established during Init.
	if len(input.Rows) > 0 && len(input.Rows[0].Children) > 0 {
		p.discarded = input.Rows[0].Children[0]
		if len(input.Rows[0].Children) > 1 {
			p.retained = input.Rows[0].Children[1]
		}
	}
	switch p.mode {
	case "error", "empty error":
		return preparationFailure
	case "panic":
		panic("preparation fixture panic")
	case "cancel", "empty cancel":
		p.cancel()
	case "add":
		input.Rows[0].Children = append(input.Rows[0].Children, &eligibleLeaf{})
	case "identity":
		*input.Rows[0].ID = 9
	case "value":
		*output.Data[0].Children[0].Name = "changed"
	case "marker":
		input.Rows[0].Children[0].Has.Name = false
	case "current":
		*input.Current[0].Name = "changed Current"
	case "reparent":
		input.Rows[1].Children = append(input.Rows[1].Children, input.Rows[0].Children[0])
		input.Rows[0].Children = nil
	case "output replacement":
		output.Data = []*eligibleParent{{}}
	case "root removal":
		input.Rows = input.Rows[:1]
	case "root reorder":
		input.Rows[0], input.Rows[1] = input.Rows[1], input.Rows[0]
	case "child reorder":
		input.Rows[0].Children[0], input.Rows[0].Children[1] = input.Rows[0].Children[1], input.Rows[0].Children[0]
	case "mutation":
		_ = hook.DML.Insert("eligible_parent", input.Rows[0])
	case "allocation":
		_ = hook.Sequence.Allocate(context.Background(), "eligible_leaf", input.Rows[0].Children, "ID")
	case "completion":
		_ = hook.Flush.Flush(context.Background(), "")
	case "mutation child":
		_, _ = hook.Invoker.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: p.mutation, Input: &eligibilityChildInput{}})
	case "reader", "reader error":
		value, err := hook.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: p.reader, Input: &preparationReadInput{}})
		if err != nil {
			return err
		}
		if len(value.(*preparationReadOutput).Data) != 1 {
			return fmt.Errorf("native lookup missing")
		}
		p.trace = append(p.trace, "reader")
	case "all prune":
		for _, row := range input.Rows {
			row.Children = nil
		}
	default:
		if len(input.Rows) > 0 {
			input.Rows[0].Children = input.Rows[0].Children[1:]
		}
	}
	return nil
}

type preparationReadInput struct{}
type preparationReadRow struct {
	ID int `sqlx:"id"`
}
type preparationReadOutput struct {
	Data []*preparationReadRow `parameter:",kind=output,in=view" view:"Lookup,table=preparation_lookup" sql:"SELECT id FROM preparation_lookup"`
}

func runPreparationFixture(t *testing.T, mode, body string) (*preparationProbe, any, error, *sqlite.Harness) {
	t.Helper()
	db := sqlite.New(t)
	p := &preparationProbe{mode: mode}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), preparationProbeKey{}, p))
	p.cancel = cancel
	t.Cleanup(cancel)
	if err := db.ExecStatements(ctx, `CREATE TABLE eligible_parent(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)`, `INSERT INTO eligible_parent VALUES(1,'old'),(2,'other')`, `CREATE TABLE eligible_leaf(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES eligible_parent(id),name TEXT NOT NULL)`, `CREATE TABLE preparation_lookup(id INTEGER PRIMARY KEY)`, `INSERT INTO preparation_lookup VALUES(1)`, `CREATE TABLE mutation_probe(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)`); err != nil {
		t.Fatal(err)
	}
	hookName := preparationLinked[0].Name()
	if strings.HasPrefix(mode, "aggregate") {
		hookName = preparationAggregateLinked.Name()
	}
	root := eligibilityTransactionComponent(t, "Preparation", "PATCH", "/preparation", "eligible_parent", hookName, reflect.TypeFor[eligibleParentInput](), reflect.TypeFor[eligibleParentOutput]())
	root.Component.Settings.ComponentCallPolicy = "buffered"
	// Runtime metadata derives the complete relation from the canonical SQLX tags.
	root.Component.RootView.Relations = []*spec.Relation{{Holder: "Children", Name: "Children", View: &spec.View{Name: "Children", EntityHooks: preparationLinked[1].Name(), Source: &spec.ViewSource{Table: "eligible_leaf"}}}}
	if strings.HasPrefix(mode, "aggregate") {
		root.Component.RootView.Relations[0].View.EntityHooks = ""
	}
	// Recompile the handler after declaring the child lifecycle.
	native, err := writer.New(root.Component, reflect.TypeFor[eligibleParentInput](), reflect.TypeFor[eligibleParentOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	root.Handler = native
	root.DataSource = dml.Source{DB: db.DB}
	root.Providers = []locator.Provider{handlerprovider.Named("view", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key == "Current" {
			id, other := 1, 2
			name, otherName := "old", "other"
			p.current = &eligibleParent{ID: &id, Name: &name}
			return []*eligibleParent{p.current, {ID: &other, Name: &otherName}}, true, nil
		}
		if key == "CurrentChildren" {
			return []*eligibleLeaf{}, true, nil
		}
		return nil, false, nil
	})}
	readSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: preparationLinked[0].PkgPath(), Name: "PreparationLookup"}, Routes: []*spec.Route{{Method: "GET", Path: "/preparation-lookup"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: readSpec, InputType: reflect.TypeFor[preparationReadInput](), OutputType: reflect.TypeFor[preparationReadOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	read, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[preparationReadInput](), OutputType: reflect.TypeFor[preparationReadOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	reader := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[preparationReadOutput](), Reader: read}
	p.reader = dexec.ComponentTarget{Component: reader.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/preparation-lookup"}}
	child := eligibilityTransactionComponent(t, "PreparationMutation", "POST", "/preparation-mutation", "mutation_probe", "", reflect.TypeFor[eligibilityChildInput](), reflect.TypeFor[eligibilityChildOutput]())
	child.DataSource = dml.Source{DB: db.DB}
	p.mutation = dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/preparation-mutation"}}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{root, reader, child})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if mode == "reader error" || mode == "schema error unavailable" {
		if _, err := db.DB.Exec("DROP TABLE preparation_lookup"); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest("PATCH", "/preparation", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	out, err := runtime.ExecuteRoute(ctx, "PATCH", "/preparation", scope)
	return p, out, err, db
}

const preparationBody = `{"data":[{"ID":1,"Children":[{"Name":"discarded"},{"ID":0,"Name":"retained"}]},{"ID":2}]}`

func TestAfterValidateInputNativeFilteringSQLite(t *testing.T) {
	for _, mode := range []string{"prune", "all prune", "reader", "empty"} {
		t.Run(mode, func(t *testing.T) {
			body := preparationBody
			if mode == "empty" {
				body = `{"data":[]}`
			}
			p, result, err, db := runPreparationFixture(t, mode, body)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			for _, v := range p.trace {
				if v == "prepare" {
					calls++
				}
			}
			if calls != 1 {
				t.Fatalf("hook calls=%d trace=%v", calls, p.trace)
			}
			if mode == "prune" {
				if p.discarded.ID != nil {
					t.Fatal("pruned row received allocation")
				}
				if p.retained.ID == nil || *p.retained.ID == 0 || p.retained.ParentID == nil || *p.retained.ParentID != 1 {
					t.Fatal("retained row allocation or parent link lost")
				}
				if !p.originalID || p.originalParentID {
					t.Fatalf("Original supplied zero/link facts changed: id=%v parent=%v", p.originalID, p.originalParentID)
				}
				if p.childInit != 2 || p.childValidate != 2 || p.childSequence != 1 {
					t.Fatalf("lifecycle rerun or stale frame: %+v", p)
				}
				rows := result.(*eligibleParentOutput).Data
				if len(rows) != 2 || len(rows[0].Children) != 1 || rows[0].Children[0] != p.retained {
					t.Fatal("response occurrence identity changed")
				}
				assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{"retained"})
			} else if mode == "all prune" {
				if p.childSequence != 0 || p.discarded.ID != nil || *p.retained.ID != 0 {
					t.Fatal("filtered graph reached allocation or sequence callback")
				}
				var n int
				if err := db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='sqlx_sequence_reservations'`).Scan(&n); err != nil || n != 0 {
					t.Fatalf("allocation infrastructure unexpectedly created: %d %v", n, err)
				}
				assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{})
			}
			assertEligibilityNames(t, context.Background(), db.DB, "eligible_parent", []string{"old", "other"})
		})
	}
}

func TestAfterValidateInputNativeFailurePrefixSQLite(t *testing.T) {
	for _, mode := range []string{"schema error", "schema error unavailable", "business error", "cancel validation", "error", "empty error", "cancel", "empty cancel", "panic", "add", "identity", "value", "marker", "current", "reparent", "output replacement", "root removal", "root reorder", "child reorder", "mutation", "allocation", "completion", "mutation child", "reader error"} {
		t.Run(mode, func(t *testing.T) {
			body := preparationBody
			if strings.HasPrefix(mode, "empty") {
				body = `{"data":[]}`
			}
			if strings.HasPrefix(mode, "schema error") {
				body = `{"data":[{"ID":1,"Children":[{"Name":null}]}]}`
			}
			p, _, err, db := runPreparationFixture(t, mode, body)
			if err == nil {
				t.Fatalf("forbidden preparation succeeded: %s", mode)
			}
			before := strings.HasPrefix(mode, "schema error") || mode == "business error" || mode == "cancel validation"
			called := false
			for _, v := range p.trace {
				if v == "prepare" {
					called = true
				}
			}
			if called == before {
				t.Fatalf("failure boundary trace=%v", p.trace)
			}
			if mode == "business error" || mode == "error" {
				if !errors.Is(err, preparationFailure) {
					t.Fatal(err)
				}
			}
			if strings.Contains(mode, "cancel") && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "mutation" || mode == "allocation" || mode == "completion" || mode == "mutation child" {
				if !errors.Is(err, engine.ErrWriteEligibilityMutation) {
					t.Fatal(err)
				}
			}
			if p.childSequence != 0 {
				t.Fatal("failed preparation reached native sequence callbacks")
			}
			assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{})
			assertEligibilityNames(t, context.Background(), db.DB, "eligible_parent", []string{"old", "other"})
			assertEligibilityNames(t, context.Background(), db.DB, "mutation_probe", []string{})
		})
	}
}

type preparationAggregateRootHook struct {
	Input *eligibleParentInput `bind:"kind=input,required"`
}

var preparationAggregateLinked = reflect.TypeFor[preparationAggregateRootHook]()

func (hook *preparationAggregateRootHook) Init(ctx context.Context, _ *eligibleParent, state h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	preparationProbeFrom(ctx).roots++
	state.Output.Data = hook.Input.Rows
	return nil
}
func (*preparationAggregateRootHook) ValidateInput(ctx context.Context, _ *eligibleParentInput, _ *eligibleParentOutput, report h.ValidationReport) error {
	p := preparationProbeFrom(ctx)
	p.trace = append(p.trace, "aggregate validate")
	if p.mode == "aggregate business" {
		report.Add(h.Violation{Message: "aggregate payload rejected", Check: "business"})
	}
	if p.mode == "aggregate operational" {
		return preparationFailure
	}
	return nil
}
func (*preparationAggregateRootHook) AfterValidateInput(ctx context.Context, input *eligibleParentInput, _ *eligibleParentOutput) error {
	p := preparationProbeFrom(ctx)
	p.trace = append(p.trace, "prepare")
	if len(input.Rows) > 0 {
		p.discarded = input.Rows[0].Children[0]
		p.retained = input.Rows[0].Children[1]
		input.Rows[0].Children = input.Rows[0].Children[1:]
	}
	return nil
}
func TestAfterValidateInputAggregateGateSQLite(t *testing.T) {
	for _, mode := range []string{"aggregate success", "aggregate schema", "aggregate business", "aggregate operational"} {
		t.Run(mode, func(t *testing.T) {
			body := preparationBody
			if mode == "aggregate schema" {
				body = `{"data":[{"ID":1,"Children":[{"Name":null}]}]}`
			}
			p, _, err, db := runPreparationFixture(t, mode, body)
			if (err == nil) != (mode == "aggregate success") {
				t.Fatalf("error=%v", err)
			}
			expected := []string{"aggregate validate"}
			if mode == "aggregate success" {
				expected = append(expected, "prepare")
			}
			if !reflect.DeepEqual(p.trace, expected) || p.childValidate != 0 {
				t.Fatalf("aggregate/row validation precedence=%v child=%d", p.trace, p.childValidate)
			}
			if mode == "aggregate success" {
				if p.discarded.ID != nil || p.retained.ID == nil || *p.retained.ID == 0 {
					t.Fatal("aggregate filtering allocation incorrect")
				}
				assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{"retained"})
			} else {
				assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{})
			}
		})
	}
}

type preparationRetryRootHook struct {
	nativeEligibilityRetryHooks
	preparations int
}

var preparationRetryLinked = reflect.TypeFor[preparationRetryRootHook]()

func (hook *preparationRetryRootHook) AfterValidateInput(ctx context.Context, input *nativeEligibilityInput, _ *nativeEligibilityOutput) error {
	hook.preparations++
	if hook.preparations != 1 {
		return fmt.Errorf("preparation replayed on one lifecycle")
	}
	p := preparationProbeFrom(ctx)
	p.trace = append(p.trace, *input.Current[0].Name)
	return nil
}
func TestAfterValidateInputFreshNativeRetrySQLite(t *testing.T) {
	db := sqlite.New(t)
	p := &preparationProbe{}
	prior := &eligibilityTransactionProbe{}
	ctx := context.WithValue(context.WithValue(context.Background(), preparationProbeKey{}, p), eligibilityTransactionProbeKey{}, prior)
	if err := db.ExecStatements(ctx, `CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`, `INSERT INTO eligible VALUES(1,'old',NULL)`, `CREATE TRIGGER ignore_preparation_update BEFORE UPDATE ON eligible WHEN NEW.name='requested' AND OLD.name='old' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	component := eligibilityTransactionComponent(t, "PreparationRetry", "PATCH", "/preparation-retry", "eligible", preparationRetryLinked.Name(), reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput]())
	request := httptest.NewRequest("PATCH", "/preparation-retry", strings.NewReader(`{"data":[{"ID":1,"Name":"requested"}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	reads, commits := 0, 0
	var refreshErr error
	source := dml.Source{DB: db.DB, OnCommit: func(ctx context.Context) {
		commits++
		if commits == 1 {
			_, refreshErr = db.DB.ExecContext(ctx, `UPDATE eligible SET name='refreshed locked' WHERE id=1`)
		}
	}}
	input, _ := component.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/preparation-retry"})
	_, err = engine.New().Execute(ctx, engine.Request{Input: input, Handler: component.Handler, Scope: scope, Providers: []locator.Provider{eligibilityCurrentProvider(db.DB, func() { reads++ })}, DataSource: source})
	if err != nil || refreshErr != nil || reads != 2 || prior.recoveries != 1 || !reflect.DeepEqual(p.trace, []string{"old", "refreshed locked"}) || !reflect.DeepEqual(prior.queueCounters, []int{1, 0}) {
		t.Fatalf("fresh attempt err=%v refresh=%v reads=%d recovery=%d preparation=%v queues=%v", err, refreshErr, reads, prior.recoveries, p.trace, prior.queueCounters)
	}
}
