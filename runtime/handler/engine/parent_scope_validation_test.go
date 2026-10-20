package engine_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

type parentScopeProbeKey struct{}
type parentScopeProbe struct {
	predicateCalls                                           int
	predicateBeforeInit                                      bool
	predicateFirstAttempt                                    error
	mode                                                     string
	invalid                                                  bool
	init, aggregate, rootSequence, sequence, queued, commits int
	previous                                                 []*eligibleLeaf
	attempted                                                error
	input                                                    *eligibleParentInput
}

func parentScopeProbeFrom(ctx context.Context) *parentScopeProbe {
	return ctx.Value(parentScopeProbeKey{}).(*parentScopeProbe)
}

type parentScopeDiagnosticPredicate struct {
	DML h.DML `bind:"kind=dml,required"`
}

var parentScopePredicateLinked = reflect.TypeFor[parentScopeDiagnosticPredicate]()

func (predicate *parentScopeDiagnosticPredicate) Compute(ctx context.Context, _ any) (*xpredicate.Criteria, error) {
	p := parentScopeProbeFrom(ctx)
	p.predicateCalls++
	if p.init == 0 {
		p.predicateBeforeInit = true
	}
	p.attempted = predicate.DML.Insert("eligible_parent", &eligibleParent{})
	if p.predicateCalls == 1 {
		p.predicateFirstAttempt = p.attempted
	}
	return nil, nil
}

type parentScopeRootHooks struct {
	Input    *eligibleParentInput `bind:"kind=input,required"`
	DML      h.DML                `bind:"kind=dml,required"`
	Sequence h.Sequencer          `bind:"kind=sequencer,required"`
}
type parentScopeChildHooks struct {
	DML h.DML `bind:"kind=dml,required"`
}

var parentScopeLinked = []reflect.Type{reflect.TypeFor[parentScopeRootHooks](), reflect.TypeFor[parentScopeChildHooks]()}

func (hook *parentScopeRootHooks) Init(ctx context.Context, row *eligibleParent, state h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	p := parentScopeProbeFrom(ctx)
	p.init++
	p.input = hook.Input
	state.Output.Data = hook.Input.Rows
	if *row.ID != 1 {
		return nil
	}
	switch p.mode {
	case "late child dml":
		id, name := 11, "requested"
		row.Children = []*eligibleLeaf{{ID: &id, Name: &name, Has: &eligibleLeafHas{ID: true, Name: true}}}
	case "prune":
		row.Children = nil
	case "reparent":
		hook.Input.Rows[1].Children = row.Children
		row.Children = nil
	case "dml":
		p.attempted = hook.DML.Insert("eligible_parent", row)
	case "allocate":
		p.attempted = hook.Sequence.Allocate(context.Background(), "eligible_leaf", row.Children, "ID")
	}
	return nil
}
func (hook *parentScopeRootHooks) ValidateInput(ctx context.Context, _ *eligibleParentInput, _ *eligibleParentOutput, report h.ValidationReport) error {
	p := parentScopeProbeFrom(ctx)
	p.aggregate++
	if p.mode == "aggregate dml" {
		p.attempted = hook.DML.Insert("eligible_parent", hook.Input.Rows[0])
	}
	if p.invalid {
		report.Add(h.Violation{Check: "business", Message: "parent scope aggregate invalid"})
	}
	return nil
}
func (*parentScopeRootHooks) AfterSequence(ctx context.Context, _ *eligibleParent, _ h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	parentScopeProbeFrom(ctx).rootSequence++
	return nil
}
func (hook *parentScopeChildHooks) Init(ctx context.Context, row *eligibleLeaf, state h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	p := parentScopeProbeFrom(ctx)
	p.previous = append(p.previous, state.Previous)
	if p.mode == "late child dml" {
		p.attempted = hook.DML.Insert("eligible_leaf", row)
	}
	return nil
}
func (*parentScopeChildHooks) AfterSequence(ctx context.Context, _ *eligibleLeaf, _ h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	parentScopeProbeFrom(ctx).sequence++
	return nil
}
func (*parentScopeChildHooks) AfterQueue(ctx context.Context, _ *eligibleLeaf, _ h.LifecycleContext[eligibleLeaf, eligibleParent, eligibleParentOutput]) error {
	parentScopeProbeFrom(ctx).queued++
	return nil
}

func runParentScopeValidation(t *testing.T, mode string, invalid, foreign bool) (*parentScopeProbe, error, *sqlite.Harness) {
	t.Helper()
	db := sqlite.New(t)
	p := &parentScopeProbe{mode: mode, invalid: invalid}
	ctx := context.WithValue(context.Background(), parentScopeProbeKey{}, p)
	if err := db.ExecStatements(ctx, `CREATE TABLE eligible_parent(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)`, `INSERT INTO eligible_parent VALUES(1,'one'),(2,'two')`, `CREATE TABLE eligible_leaf(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES eligible_parent(id),name TEXT NOT NULL)`, `INSERT INTO eligible_leaf VALUES(11,2,'foreign old')`, `CREATE TABLE scope_audit(id INTEGER PRIMARY KEY AUTOINCREMENT)`, `CREATE TRIGGER scope_parent_update AFTER UPDATE ON eligible_parent BEGIN INSERT INTO scope_audit(id) VALUES(NULL); END`); err != nil {
		t.Fatal(err)
	}
	root := eligibilityTransactionComponent(t, "ParentScopeValidation", "PATCH", "/parent-scope-validation", "eligible_parent", parentScopeLinked[0].Name(), reflect.TypeFor[eligibleParentInput](), reflect.TypeFor[eligibleParentOutput]())
	root.Component.RootView.Relations = []*spec.Relation{{Holder: "Children", Name: "Children", View: &spec.View{Name: "Children", EntityHooks: parentScopeLinked[1].Name(), Source: &spec.ViewSource{Table: "eligible_leaf"}}}}
	if mode == "predicate dml" {
		group := 7
		child := root.Component.RootView.Relations[0].View
		child.MutationPredicateGroup = &group
		child.OnDeleteNotFound = "ignore"
		root.Component.Parameters = append(root.Component.Parameters, &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}, Predicates: []*spec.Predicate{{Group: group, Name: "handler", Args: []string{parentScopePredicateLinked.PkgPath() + "." + parentScopePredicateLinked.Name()}}}})
	}
	native, err := writer.New(root.Component, reflect.TypeFor[eligibleParentInput](), reflect.TypeFor[eligibleParentOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	root.Handler = native
	root.DataSource = dml.Source{DB: db.DB, OnCommit: func(context.Context) { p.commits++ }}
	root.Providers = []locator.Provider{handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, key string) (any, bool, error) {
		switch key {
		case "Current":
			rows, err := db.DB.QueryContext(ctx, "SELECT id,name FROM eligible_parent ORDER BY id")
			if err != nil {
				return nil, true, err
			}
			defer rows.Close()
			var result []*eligibleParent
			for rows.Next() {
				r := &eligibleParent{ID: new(int), Name: new(string)}
				if err := rows.Scan(r.ID, r.Name); err != nil {
					return nil, true, err
				}
				result = append(result, r)
			}
			return result, true, rows.Err()
		case "CurrentChildren":
			rows, err := db.DB.QueryContext(ctx, "SELECT id,parent_id,name FROM eligible_leaf ORDER BY id")
			if err != nil {
				return nil, true, err
			}
			defer rows.Close()
			var result []*eligibleLeaf
			for rows.Next() {
				r := &eligibleLeaf{ID: new(int), ParentID: new(int), Name: new(string)}
				if err := rows.Scan(r.ID, r.ParentID, r.Name); err != nil {
					return nil, true, err
				}
				result = append(result, r)
			}
			return result, true, rows.Err()
		}
		return nil, false, nil
	})}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	body := `{"data":[{"ID":1,"Name":"one requested","Children":[{"ID":11,"Name":"requested"}]},{"ID":2}]}`
	if mode == "predicate dml" {
		body = `{"data":[{"ID":1,"Name":"one requested","Children":[{"ID":11,"Name":"requested"},{"ID":99,"Remove":true}]},{"ID":2}]}`
	}
	if mode == "late child dml" {
		body = `{"data":[{"ID":1,"Name":"one requested"},{"ID":2}]}`
	}
	if mode == "schema" {
		body = `{"data":[{"ID":1,"Children":[{"ID":11,"Name":null}]},{"ID":2}]}`
	}
	if !foreign {
		body = `{"data":[{"ID":1,"Name":"one requested"},{"ID":2,"Children":[{"ID":11,"Name":"requested"},{"Name":"new"}]}]}`
	}
	request := httptest.NewRequest("PATCH", "/parent-scope-validation", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	_, err = runtime.ExecuteRoute(ctx, "PATCH", "/parent-scope-validation", scope)
	return p, err, db
}

func TestParentScopeValidationPrecedenceSQLite(t *testing.T) {
	for _, mode := range []string{"valid", "schema", "business", "prune", "reparent", "dml", "allocate", "aggregate dml", "late child dml", "predicate dml"} {
		t.Run(mode, func(t *testing.T) {
			invalid := mode == "business" || mode == "dml" || mode == "allocate" || mode == "aggregate dml" || mode == "late child dml" || mode == "predicate dml"
			p, err, db := runParentScopeValidation(t, mode, invalid, true)
			if err == nil {
				t.Fatal("foreign Previous accepted")
			}
			var validation *h.Validation
			isValidation := errors.As(err, &validation)
			if mode == "business" {
				if !isValidation || !strings.Contains(err.Error(), "parent scope aggregate invalid") {
					t.Fatalf("aggregate validation must precede scope rejection: %v", err)
				}
			} else if mode == "schema" {
				if !isValidation || strings.Contains(err.Error(), "outside parent scope") {
					t.Fatalf("native validation must precede scope rejection: %v", err)
				}
			} else if mode == "dml" || mode == "allocate" || mode == "aggregate dml" || mode == "late child dml" || mode == "predicate dml" {
				if !errors.Is(p.attempted, engine.ErrWriteEligibilityMutation) || !errors.Is(err, engine.ErrWriteEligibilityMutation) || isValidation {
					t.Fatalf("swallowed managed operation must remain fatal: attempted=%v error=%v", p.attempted, err)
				}
			} else if isValidation || !strings.Contains(err.Error(), "outside parent scope") {
				t.Fatalf("sticky scope mismatch lost: %v", err)
			}
			if mode == "predicate dml" && (p.predicateCalls == 0 || !p.predicateBeforeInit || !errors.Is(p.predicateFirstAttempt, engine.ErrWriteEligibilityMutation)) {
				t.Fatalf("predicate did not attempt managed DML during initial framing: %+v", p)
			}
			if p.init != 2 || p.aggregate != 1 || p.rootSequence != 0 || p.sequence != 0 || p.queued != 0 || p.commits != 0 {
				t.Fatalf("unexpected lifecycle: %+v error=%v", p, err)
			}
			if len(p.previous) == 0 {
				t.Fatal("child Init did not observe Previous")
			}
			for _, previous := range p.previous {
				if previous == nil || previous.ID == nil || *previous.ID != 11 || previous.ParentID == nil || *previous.ParentID != 2 || previous.Name == nil || *previous.Name != "foreign old" {
					t.Fatalf("genuine Previous changed: %+v", previous)
				}
			}
			assertEligibilityNames(t, context.Background(), db.DB, "eligible_parent", []string{"one", "two"})
			assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{"foreign old"})
			var audit int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM scope_audit").Scan(&audit); err != nil || audit != 0 {
				t.Fatalf("mutation audit=%d error=%v", audit, err)
			}
			var reservations int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sqlx_sequence_reservations'").Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("allocator started: tables=%d error=%v", reservations, err)
			}
		})
	}
}
func TestParentScopeValidationOrdinaryWriterSQLite(t *testing.T) {
	p, err, db := runParentScopeValidation(t, "ordinary", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if p.init != 2 || p.aggregate != 1 || p.rootSequence != 2 || p.sequence != 2 || p.queued != 2 || p.commits != 1 {
		t.Fatalf("ordinary lifecycle changed: %+v", p)
	}
	assertEligibilityNames(t, context.Background(), db.DB, "eligible_parent", []string{"one requested", "two"})
	assertEligibilityNames(t, context.Background(), db.DB, "eligible_leaf", []string{"requested", "new"})
}
