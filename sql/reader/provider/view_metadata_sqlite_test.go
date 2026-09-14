package provider

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/compiler"
	xhandler "github.com/viant/xdatly/handler"
)

type providerMetadataRow struct {
	ID   int     `sqlx:"id"`
	Name *string `sqlx:"name"`
}
type providerMetadataInput struct{ Rows []providerMetadataRow }

func TestViewProviderMetadataOptInUsesOneTypedSQLiteRead(t *testing.T) {
	for _, tc := range []struct {
		name, query            string
		observe, hasName, fail bool
	}{
		{"ordinary read", "SELECT counted(id) AS id,name FROM records ORDER BY records.id", false, true, false},
		{"observed loaded null", "SELECT counted(id) AS id,name FROM records ORDER BY records.id", true, true, false},
		{"observed omitted field", "SELECT counted(id) AS id FROM records ORDER BY records.id", true, false, false},
		{"ordinary query error", "SELECT missing FROM absent_records", false, false, true},
		{"observed query error", "SELECT missing FROM absent_records", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first'),(2,NULL)"); err != nil {
				t.Fatal(err)
			}
			var reads atomic.Int32
			connection, err := h.DB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = connection.Raw(func(raw any) error {
				return raw.(*sqlite3.SQLiteConn).RegisterFunc("counted", func(id int64) int64 { reads.Add(1); return id }, false)
			})
			closeErr := connection.Close()
			if err != nil {
				t.Fatal(err)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			bindings := []bindly.BindingSpec{{Path: "Rows", Name: "Rows", Location: bindstate.Location{Kind: "view", In: "Current"}}}
			seed, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			inputType := reflect.TypeOf(providerMetadataInput{})
			inputPlan, err := seed.CompilePlan(inputType, bindings...)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := inputPlan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			allowNulls := true
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Current"}}}, Views: []*spec.View{{Name: "Current", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: tc.query}}}}
			dependencies, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
			if err != nil {
				t.Fatal(err)
			}
			views, err := New(Config{Dependencies: dependencies, Input: projection, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			injector, err := seed.ForScope(views)
			if err != nil {
				t.Fatal(err)
			}
			input := &providerMetadataInput{}
			var events []bindly.BindingEvent
			options := []bindly.BindOption{bindly.WithPlan(inputPlan)}
			if tc.observe {
				options = append(options, bindly.WithBindingObserver(func(_ context.Context, event bindly.BindingEvent) error { events = append(events, event); return nil }))
			}
			err = injector.Bind(ctx, input, options...)
			if tc.fail {
				if err == nil || len(events) != 0 || input.Rows != nil {
					t.Fatalf("error=%v events=%+v input=%+v", err, events, input)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if reads.Load() != 2 {
				t.Fatalf("source evaluated %d rows; expected one two-row read", reads.Load())
			}
			if len(input.Rows) != 2 || input.Rows[0].ID != 1 || input.Rows[1].ID != 2 || input.Rows[1].Name != nil {
				t.Fatalf("typed rows=%+v", input.Rows)
			}
			if (input.Rows[0].Name != nil) != tc.hasName {
				t.Fatalf("selected Name=%v", input.Rows[0].Name)
			}
			if !tc.observe {
				if len(events) != 0 {
					t.Fatal("ordinary read observed metadata")
				}
				return
			}
			if len(events) != 1 || events[0].Target != input || events[0].Path != "Rows" || reflect.TypeOf(events[0].Value) != reflect.TypeOf(input.Rows) {
				t.Fatalf("events=%+v", events)
			}
			metadata, ok := events[0].Metadata.(xhandler.ReadProjection)
			if !ok || !metadata.DirectOutput() || metadata.RootHolder() != "" {
				t.Fatalf("metadata=%T", events[0].Metadata)
			}
			for i := range input.Rows {
				fields, err := metadata.Fields(i)
				if err != nil {
					t.Fatal(err)
				}
				if !fields.Has("ID") || fields.Has("Name") != tc.hasName {
					t.Fatalf("row%d projection differs", i)
				}
			}
			if _, ok := events[0].Value.(locator.ValueWithMetadata); ok {
				t.Fatal("provider envelope leaked into ordinary binding")
			}
			first := "first"
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []providerMetadataRow{{ID: 1, Name: &first}, {ID: 2}})
		})
	}
}

func TestViewMetadataTargetValidationPrecedesExecution(t *testing.T) {
	rowType := reflect.TypeOf([]providerMetadataRow{})
	// No execution is installed: both checks must finish before any SQL read.
	valueLocator := &viewLocator{views: map[string]*boundView{"Current": {targetType: rowType}}}
	for _, tc := range []struct {
		name    string
		target  reflect.Type
		invalid bool
	}{
		{"Missing", rowType, false},
		{"Current", reflect.TypeOf([]string{}), true},
	} {
		value, found, err := valueLocator.ValueInScope(context.Background(), nil, tc.target, tc.name)
		if (err != nil) != tc.invalid || found || value != nil {
			t.Fatalf("%s value=%v found=%v err=%v", tc.name, value, found, err)
		}
	}
}
