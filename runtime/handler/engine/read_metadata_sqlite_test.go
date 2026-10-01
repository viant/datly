package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/reader/compiler"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	xhandler "github.com/viant/xdatly/handler"
)

type metadataSQLiteRow struct {
	ID   int     `sqlx:"id"`
	Name *string `sqlx:"name"`
}
type metadataSQLiteInput struct {
	Current     []metadataSQLiteRow
	initialized bool
}

func (i *metadataSQLiteInput) Init(ctx context.Context) error {
	metadata, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok {
		return fmt.Errorf("missing read metadata during Init")
	}
	projection, err := metadata.Projection("Current")
	if err != nil {
		return err
	}
	for index := range i.Current {
		if _, err := projection.Fields(index); err != nil {
			return err
		}
	}
	i.initialized = true
	return nil
}

type metadataSQLiteOutput struct {
	IDs        []int
	NameLoaded []bool
}
type metadataSQLiteContract struct{}

func (*metadataSQLiteContract) RequiresReadMetadata() bool { return true }
func (*metadataSQLiteContract) CaptureInput(ctx context.Context, input *metadataSQLiteInput) (any, error) {
	if input.initialized {
		return nil, fmt.Errorf("capture after initialization")
	}
	metadata, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing read metadata during capture")
	}
	projection, err := metadata.Projection("Current")
	if err != nil {
		return nil, err
	}
	result := &metadataSQLiteOutput{}
	for index, row := range input.Current {
		fields, err := projection.Fields(index)
		if err != nil {
			return nil, err
		}
		if !fields.Has("ID") {
			return nil, fmt.Errorf("identity evidence missing")
		}
		result.IDs = append(result.IDs, row.ID)
		result.NameLoaded = append(result.NameLoaded, fields.Has("Name"))
	}
	return result, nil
}
func (*metadataSQLiteContract) Exec(ctx context.Context, session xhandler.Session, input *metadataSQLiteInput, output *metadataSQLiteOutput) error {
	if !input.initialized {
		return fmt.Errorf("input was not initialized")
	}
	value, found, err := session.Binder().Lookup(ctx, xhandler.InputSnapshotKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("snapshot missing")
	}
	*output = *value.(*metadataSQLiteOutput)
	return nil
}

func TestEngineReadMetadataSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, query  string
		loaded, fail bool
	}{
		{"loaded SQL null", "SELECT id,name FROM records ORDER BY id", true, false},
		{"omitted field", "SELECT id FROM records ORDER BY id", false, false},
		{"SQL failure", "SELECT missing FROM records", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first'),(2,NULL)"); err != nil {
				t.Fatal(err)
			}
			bindings := []bindly.BindingSpec{{Path: "Current", Name: "Current", Location: bindstate.Location{Kind: "view", In: "CurrentView"}}}
			seed, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			inputType := reflect.TypeOf(metadataSQLiteInput{})
			plan, err := seed.CompilePlan(inputType, bindings...)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := plan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			allowNulls := true
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "CurrentView"}}}, Views: []*spec.View{{Name: "CurrentView", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: tc.query}}}}
			dependencies, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
			if err != nil {
				t.Fatal(err)
			}
			views, err := viewprovider.New(viewprovider.Config{Dependencies: dependencies, Input: projection, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			request := Request{Input: testRouteInputWithPlan(t, inputType, plan, bindings...), Handler: custom.New[metadataSQLiteInput, metadataSQLiteOutput](&metadataSQLiteContract{}), Providers: []locator.Provider{views}}
			actual, err := New().Execute(ctx, request)
			if tc.fail {
				if err == nil {
					t.Fatal("SQL error swallowed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := &metadataSQLiteOutput{IDs: []int{1, 2}, NameLoaded: []bool{tc.loaded, tc.loaded}}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("got %+v want %+v", actual, want)
			}
			// A second invocation must not reuse stale rows or evidence.
			if err := h.ExecStatements(ctx, "DELETE FROM records"); err != nil {
				t.Fatal(err)
			}
			actual, err = New().Execute(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if got := actual.(*metadataSQLiteOutput); len(got.IDs) != 0 || len(got.NameLoaded) != 0 {
				t.Fatalf("stale invocation: %+v", got)
			}
		})
	}
}

type conditionalMetadataInput struct {
	Fail    bool
	Enabled bool                `parameter:"Enabled,kind=query,in=enabled"`
	Current []metadataSQLiteRow `parameter:"Current,kind=view,in=CurrentView,when=Enabled"`
}
type conditionalMetadataContract struct{}

func (*conditionalMetadataContract) RequiresReadMetadata() bool { return true }
func (*conditionalMetadataContract) CaptureInput(ctx context.Context, input *conditionalMetadataInput) (any, error) {
	reads, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("read metadata missing")
	}
	projection, err := reads.Projection("Current")
	if err != nil {
		return nil, err
	}
	if !input.Enabled {
		if _, err = projection.Fields(0); err == nil {
			return nil, fmt.Errorf("skipped read claimed loaded fields")
		}
	}
	for n := range input.Current {
		fields, err := projection.Fields(n)
		if err != nil || !fields.Has("ID") {
			return nil, fmt.Errorf("active read identity evidence: %v", err)
		}
	}
	return projection, nil
}
func (*conditionalMetadataContract) Exec(ctx context.Context, session xhandler.Session, input *conditionalMetadataInput, output *int) error {
	value, found, err := session.Binder().Lookup(ctx, xhandler.DataKey)
	if err != nil || !found {
		return fmt.Errorf("data unavailable: %v", err)
	}
	data := value.(xhandler.Data)
	if err = data.Execute("INSERT INTO audit(id) VALUES(1)"); err != nil {
		return err
	}
	*output = len(input.Current)
	if input.Fail {
		return fmt.Errorf("late business failure")
	}
	return nil
}
func TestConditionalAuxiliaryReadMetadataSQLite(t *testing.T) {
	for _, tc := range []struct{ enabled, fail bool }{{false, false}, {true, false}, {true, true}} {
		enabled := tc.enabled
		t.Run(fmt.Sprintf("enabled%v/failure%v", enabled, tc.fail), func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'one')", "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			inputType := reflect.TypeFor[conditionalMetadataInput]()
			bindings := []bindly.BindingSpec{{Path: "Current", Name: "Current", Location: bindstate.Location{Kind: "view", In: "CurrentView"}, When: "Enabled"}}
			seed, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := seed.CompilePlan(inputType, bindings...)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := plan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			query := "SELECT id,name FROM records"
			// This must never reach SQL when the condition is false.
			if !enabled {
				query = "SELECT id,name FROM nonexistent_inactive_table"
			}
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "CurrentView"}, When: "Enabled"}}, Views: []*spec.View{{Name: "CurrentView", Source: &spec.ViewSource{SQL: query}}}}
			dependencies, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
			if err != nil {
				t.Fatal(err)
			}
			views, err := viewprovider.New(viewprovider.Config{Dependencies: dependencies, Input: projection, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := New().Execute(ctx, Request{Input: testRouteInputWithPlan(t, inputType, plan, bindings...), BoundInput: &conditionalMetadataInput{Enabled: enabled, Fail: tc.fail}, DataSource: sqldml.Source{DB: h.DB}, Handler: custom.New[conditionalMetadataInput, int](&conditionalMetadataContract{}), Providers: []locator.Provider{views}})
			var auditCount int
			if queryErr := h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&auditCount); queryErr != nil {
				t.Fatal(queryErr)
			}
			if tc.fail {
				if err == nil || auditCount != 0 {
					t.Fatalf("rollback/error lost: count%d err%v", auditCount, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if auditCount != 1 {
				t.Fatal("successful conditional invocation did not commit")
			}
			want := 0
			if enabled {
				want = 1
			}
			if *actual.(*int) != want {
				t.Fatalf("rows%d want%d", *actual.(*int), want)
			}
		})
	}
}
