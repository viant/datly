package engine_test

import (
	"context"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	dtag "github.com/viant/datly/tag"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type reusedPreparationRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}

func (*reusedPreparationRow) Init(context.Context) error {
	panic("writer entity initialization must not run")
}

type reusedPreparationInput struct {
	Rows    []*reusedPreparationRow `parameter:"Rows,kind=body,in=data" view:"records,finiteReconciliation=eyJtb2RlIjoic2FtZS1wYXJlbnQtcm9vdC1maXJzdCIsInJvbGVzIjpbeyJob2xkZXIiOiJMaW5lcyIsImZpZWxkcyI6WyJQYXJlbnRJZCJdfV19"`
	Current []*reusedPreparationRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records" sql:"SELECT id,name FROM records WHERE id = :ID"`
	ID      int                     `parameter:"ID,kind=query,in=id"`
}
type reusedPreparationOutput struct{ Input *reusedPreparationInput }
type reusedPreparationHandler struct{}

func (*reusedPreparationHandler) Exec(_ context.Context, _ h.Session, input *reusedPreparationInput, output *reusedPreparationOutput) error {
	output.Input = input
	return nil
}
func TestSourceLessCustomBodyReusesSupplementalSQLiteReads(t *testing.T) {
	db := sqlite.New(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
		t.Fatal(err)
	}
	typ := reflect.TypeFor[reusedPreparationInput]()
	component, err := (&bootstrap.RouteSource{PackagePath: "example.com/preparation", HolderType: "Preparation", Tag: structTagForPreparation()}).Resolve(typ, reflect.TypeFor[reusedPreparationOutput]())
	if err != nil {
		t.Fatal(err)
	}
	native := custom.New[reusedPreparationInput, reusedPreparationOutput](&reusedPreparationHandler{})
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: typ, OutputType: reflect.TypeFor[reusedPreparationOutput](), Handler: native, HandlerOwnedOutput: true})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/prepare"})
	if !ok {
		t.Fatal("missing route")
	}
	projection, err := route.Plan().Projection()
	if err != nil {
		t.Fatal(err)
	}
	views, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: projection, SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{1, 2} {
		input := &reusedPreparationInput{ID: id, Rows: []*reusedPreparationRow{{ID: 99, Name: "must not write"}}}
		result, err := engine.New().Execute(t.Context(), engine.Request{Input: route, BoundInput: input, Handler: native, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db.DB}})
		if err != nil {
			t.Fatal(err)
		}
		got := result.(*reusedPreparationOutput)
		if got.Input != input || len(input.Current) != 1 || input.Current[0].ID != id {
			t.Fatalf("supplemental read missing/stale: %+v", input.Current)
		}
	}
	var count int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 2 {
		t.Fatalf("preparation mutated database: count=%d err=%v", count, err)
	}
	var name string
	if err := db.DB.QueryRow("SELECT name FROM records WHERE id=1").Scan(&name); err != nil || name != "one" {
		t.Fatal("preparation updated records")
	}
}

func structTagForPreparation() dtag.Component {
	return dtag.Component{Method: "POST", Path: "/prepare", Handler: "example.Prepare"}
}
