package engine_test

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func TestAuxiliaryNullTypedBodyProviderReplayRetainsNullSlots(t *testing.T) {
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
	var body []*auxiliaryReplayRow
	source := dml.Source{DB: db.DB, OnCommit: func(ctx context.Context) {
		commits++
		if commits == 1 {
			_, fixtureErr = db.DB.ExecContext(ctx, "UPDATE records SET name='refreshed' WHERE id=1")
			body[0].Evidence = nil
			body[0].Name = new(string)
			*body[0].Name = "tampered after capture"
		}
	}}
	bodyReads := 0
	id, evidenceID, parentID, requested := 1, 7, 1, "requested"
	body = []*auxiliaryReplayRow{{ID: &id, Name: &requested, Evidence: []*auxiliaryReplayEvidence{nil, {ID: &evidenceID, ParentID: &parentID}, nil}, Has: &auxiliaryReplayHas{ID: true, Name: true, Evidence: true}}}
	typedBody := handlerprovider.Named("body", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key != "data" {
			return nil, false, nil
		}
		bodyReads++
		if bodyReads > 2 {
			return nil, true, fmt.Errorf("typed body provider re-read during replay")
		}
		return body, true, nil
	})
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/auxiliary-replay"})
	if !ok {
		t.Fatal("missing actual route")
	}
	probe := &auxiliaryReplayProbe{}
	ctx = context.WithValue(ctx, auxiliaryReplayKey{}, probe)
	observations := 0
	result, err := engine.New().Execute(ctx, engine.Request{Input: route, Handler: native, Providers: []locator.Provider{provider, typedBody}, DataSource: source, Completion: func(h.Outcome) { observations++ }})
	if err != nil || fixtureErr != nil {
		t.Fatalf("native=%v fixture=%v", err, fixtureErr)
	}
	// Initial binding and CaptureSources each resolve once; replay resolves the frozen source.
	if bodyReads != 2 {
		t.Fatalf("typed body resolved %d times across native replay", bodyReads)
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
