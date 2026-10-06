package engine_test

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type auxiliaryReplayEvidence struct {
	ID       *int `sqlx:"id,primaryKey"`
	ParentID *int `sqlx:"parent_id"`
}
type auxiliaryReplayHas struct{ ID, Name, Evidence bool }
type auxiliaryReplayRow struct {
	ID       *int                       `sqlx:"id,primaryKey,autoincrement"`
	Name     *string                    `sqlx:"name"`
	Evidence []*auxiliaryReplayEvidence `view:"Evidence,table=evidence,auxiliary=true,nestedNullPolicy=skip-auxiliary" on:"ID=ParentID"`
	Has      *auxiliaryReplayHas        `setMarker:"true" sqlx:"-" json:"-"`
}
type auxiliaryReplayInput struct {
	Rows        []*auxiliaryReplayRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	CurrentRows []*auxiliaryReplayRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records"`
}
type auxiliaryReplayOutput struct {
	Data []*auxiliaryReplayRow `parameter:"Data,kind=output,in=body"`
}
type auxiliaryReplayProbe struct {
	inits, recoveries int
	previous          []string
	outputs           []*auxiliaryReplayOutput
	outcomes          []h.Outcome
}
type auxiliaryReplayKey struct{}
type auxiliaryReplayHooks struct{}

var auxiliaryReplayLinked = reflect.TypeFor[auxiliaryReplayHooks]()

func (*auxiliaryReplayHooks) Init(ctx context.Context, row *auxiliaryReplayRow, state h.LifecycleContext[auxiliaryReplayRow, h.NoParent, auxiliaryReplayOutput]) error {
	p := ctx.Value(auxiliaryReplayKey{}).(*auxiliaryReplayProbe)
	p.inits++
	if state.Previous == nil || state.Previous.Name == nil {
		return fmt.Errorf("missing refreshed Current")
	}
	p.previous = append(p.previous, *state.Previous.Name)
	if len(row.Evidence) != 3 || row.Evidence[0] != nil || row.Evidence[1] == nil || row.Evidence[2] != nil || !row.Has.Evidence || !state.Original.Has("Name") {
		return fmt.Errorf("replay witness: evidence=%+v has=%+v originalName=%v", row.Evidence, row.Has, state.Original.Has("Name"))
	}
	p.outputs = append(p.outputs, state.Output)
	return nil
}
func (*auxiliaryReplayHooks) Recover(ctx context.Context, _ *auxiliaryReplayInput, _ *auxiliaryReplayOutput, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
	if outcome.Attempt != 0 || outcome.RetryLimit != 1 || !outcome.CommitConfirmed() || outcome.Mutation.Affected != 0 {
		return rhandler.RecoveryNone, fmt.Errorf("unexpected actual mutation recovery: %+v", outcome)
	}
	ctx.Value(auxiliaryReplayKey{}).(*auxiliaryReplayProbe).recoveries++
	return rhandler.RecoveryRetry, nil
}
func (*auxiliaryReplayHooks) Finalize(ctx context.Context, _ *auxiliaryReplayInput, _ *auxiliaryReplayOutput, outcome h.Outcome) error {
	p := ctx.Value(auxiliaryReplayKey{}).(*auxiliaryReplayProbe)
	p.outcomes = append(p.outcomes, outcome)
	return nil
}
func TestAuxiliaryNullNativeMutationRecoveryRetainsBodyAndFreshCurrent(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)`, `INSERT INTO records VALUES(1,'old')`, `CREATE TABLE evidence(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES records(id))`, `INSERT INTO evidence VALUES(7,1)`, `CREATE TRIGGER ignore_first BEFORE UPDATE ON records WHEN OLD.name='old' AND NEW.name='requested' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: auxiliaryReplayLinked.PkgPath(), Name: "AuxiliaryReplay"}, Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/auxiliary-replay"}}, RootView: &spec.View{Name: "Rows", EntityHooks: auxiliaryReplayLinked.Name(), Source: &spec.ViewSource{Table: "records"}, Relations: []*spec.Relation{{Name: "Evidence", Holder: "Evidence", View: &spec.View{Name: "Evidence", Auxiliary: true, NestedNullPolicy: "skip-auxiliary", Source: &spec.ViewSource{Table: "evidence"}}}}}}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[auxiliaryReplayInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	native, err := writer.New(c, reflect.TypeFor[auxiliaryReplayInput](), reflect.TypeFor[auxiliaryReplayOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	reads, commits := 0, 0
	var fixtureErr error
	provider := handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key != "CurrentRows" {
			return nil, false, nil
		}
		reads++
		row := &auxiliaryReplayRow{ID: new(int), Name: new(string)}
		err := db.DB.QueryRowContext(ctx, "SELECT id,name FROM records WHERE id=1").Scan(row.ID, row.Name)
		return []*auxiliaryReplayRow{row}, true, err
	})
	source := dml.Source{DB: db.DB, OnCommit: func(ctx context.Context) {
		commits++
		if commits == 1 {
			_, fixtureErr = db.DB.ExecContext(ctx, "UPDATE records SET name='refreshed' WHERE id=1")
		}
	}}
	request := httptest.NewRequest("PATCH", "/auxiliary-replay", strings.NewReader(`{"data":[{"ID":1,"Name":"requested","Evidence":[null,{"ID":7,"ParentID":1},null]}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/auxiliary-replay"})
	if !ok {
		t.Fatal("missing actual route")
	}
	probe := &auxiliaryReplayProbe{}
	ctx = context.WithValue(ctx, auxiliaryReplayKey{}, probe)
	observations := 0
	result, err := engine.New().Execute(ctx, engine.Request{Input: route, Handler: native, Scope: scope, Providers: []locator.Provider{provider}, DataSource: source, Completion: func(h.Outcome) { observations++ }})
	if err != nil || fixtureErr != nil {
		t.Fatalf("native=%v fixture=%v", err, fixtureErr)
	}
	if reads != 2 || commits != 2 || probe.inits != 2 || probe.recoveries != 1 || observations != 1 || len(probe.outcomes) != 2 || probe.outcomes[0].Error == nil || probe.outcomes[1].Error != nil {
		t.Fatalf("reads=%d commits=%d inits=%d recoveries=%d observations=%d outcomes=%+v", reads, commits, probe.inits, probe.recoveries, observations, probe.outcomes)
	}
	if !reflect.DeepEqual(probe.previous, []string{"old", "refreshed"}) {
		t.Fatalf("Current did not refresh: %v", probe.previous)
	}
	if len(probe.outputs) != 2 || probe.outputs[0] == probe.outputs[1] || result != probe.outputs[1] {
		t.Fatal("attempt output state reused")
	}
	out := result.(*auxiliaryReplayOutput)
	if len(out.Data) != 1 || len(out.Data[0].Evidence) != 3 || out.Data[0].Evidence[0] != nil || out.Data[0].Evidence[2] != nil {
		t.Fatal("replayed response compacted")
	}
	var name string
	var count int
	if err = db.DB.QueryRowContext(ctx, "SELECT name FROM records WHERE id=1").Scan(&name); err != nil || name != "requested" {
		t.Fatalf("final record=%s/%v", name, err)
	}
	if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM evidence WHERE id=7 AND parent_id=1").Scan(&count); err != nil || count != 1 {
		t.Fatal("auxiliary evidence altered")
	}
}
