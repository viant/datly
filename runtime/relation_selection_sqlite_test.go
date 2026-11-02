package runtime_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
)

func relationSelectionFixture(t *testing.T) (*druntime.Runtime, exec.ComponentTarget, *sql.DB, reflect.Type, string) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "relations.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	_, err = db.ExecContext(ctx, `CREATE TABLE facts(id INTEGER,units INTEGER);CREATE TABLE labels(id INTEGER,label TEXT);CREATE TABLE unused(id INTEGER,label TEXT);
 INSERT INTO facts VALUES(1,400),(1,500),(2,1000),(2,1010);INSERT INTO labels VALUES(1,'One'),(2,'Two');INSERT INTO unused VALUES(1,'Unused');`)
	require.NoError(t, err)
	source := `#package('example.com/neutral/relations')
#setting($_ = $route('/records','GET'))
#define($_ = $Data<?>(output/view))
SELECT records.*,label.*,unused.*,cast(records.units AS int),tag(records.id,'groupable:"true"'),tag(records.units,'groupable:"false"'),selector_fields(records,true),selector_order_by(records,true),selector_limit(records,true),selector_offset(records,true),grouping_enabled(records)
FROM (SELECT f.id,SUM(f.units) AS units FROM (SELECT id,units,COUNT(*) AS copies FROM facts GROUP BY id,units) f GROUP BY f.id,f.units,f.copies) records
JOIN (SELECT id,label FROM labels) label ON label.id=records.id AND 1=1
JOIN (SELECT id,label FROM unused) unused ON unused.id=records.id AND 1=1`
	contract, err := transcribe.NewCompiler().RuntimeContractsInModule(ctx, "example.com/neutral", &transcribe.Source{Scope: "example.com/neutral/relations", Name: "records", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db})})
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	registered, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { runtime.Shutdown(ctx) })
	return runtime, exec.ComponentTarget{Component: registered.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}, db, contract.InputType, registered.Component.RootView.Name
}
func TestRelationSelectionPreservesRawSourceAndPrunesChildren(t *testing.T) {
	runtime, target, db, inputType, view := relationSelectionFixture(t)
	_, err := db.Exec("DROP TABLE unused")
	require.NoError(t, err)
	invoke := func(selection *exec.RelationSelection) (any, error) {
		return runtime.InvokeComponent(context.Background(), exec.ComponentRequest{Target: target, Input: reflect.New(inputType).Interface(), ReaderOptions: &exec.ReaderOptions{Relations: selection}})
	}
	_, err = invoke(nil)
	require.ErrorContains(t, err, "unused", "absent retains ordinary full relation loading")
	for _, tc := range []struct {
		name      string
		holders   []string
		wantLabel bool
	}{{"none", nil, false}, {"label", []string{"Label"}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := invoke(exec.NewRelationSelection(target, view, tc.holders...))
			require.NoError(t, err)
			rows := reflect.Indirect(reflect.ValueOf(out)).FieldByName("Data")
			require.Equal(t, 4, rows.Len(), "relation selection must not regroup scalar SQL")
			for i := 0; i < rows.Len(); i++ {
				row := reflect.Indirect(rows.Index(i))
				require.Equal(t, !tc.wantLabel, row.FieldByName("Label").IsNil())
				require.True(t, row.FieldByName("Unused").IsNil())
			}
		})
	}
	_, err = invoke(exec.NewRelationSelection(target, "foreign", "Label"))
	require.ErrorContains(t, err, "unknown view")
	_, err = invoke(exec.NewRelationSelection(target, view, "Missing"))
	require.ErrorContains(t, err, "unknown public relation selection")
	_, err = invoke(exec.NewRelationSelection(target, view, "units"))
	require.ErrorContains(t, err, "unknown public relation selection")
	// Ordinary scalar projection remains authoritative. Relation selection only
	// restricts its graph; it cannot add keys or change the grouped SQL.
	output, err := runtime.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: target, Input: reflect.New(inputType).Interface(),
		ReaderOptions: &exec.ReaderOptions{Relations: exec.NewRelationSelection(target, view, "Label")},
		Providers:     []locator.Provider{provider.Static(handler.SelectorsKey, state.Selectors{&state.NamedSelector{Name: view, Selector: state.Selector{Fields: []string{"id", "units"}, OrderBy: "units DESC", Limit: 1}}})},
	})
	require.NoError(t, err)
	rows := reflect.Indirect(reflect.ValueOf(output)).FieldByName("Data")
	require.Equal(t, 1, rows.Len())
	row := reflect.Indirect(rows.Index(0))
	require.EqualValues(t, 2010, row.FieldByName("Units").Interface())
	require.True(t, row.FieldByName("Label").IsNil())
	// Concurrent invocation selections share immutable registered graph metadata.
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(selected bool) {
			defer wg.Done()
			var names []string
			if selected {
				names = []string{"Label"}
			}
			out, err := invoke(exec.NewRelationSelection(target, view, names...))
			if !assertRelationOutput(out, err, selected) {
				t.Errorf("concurrent selection=%v error=%v", selected, err)
			}
		}(i%2 == 0)
	}
	wg.Wait()
}
func assertRelationOutput(out any, err error, selected bool) bool {
	if err != nil {
		return false
	}
	rows := reflect.Indirect(reflect.ValueOf(out)).FieldByName("Data")
	if rows.Len() != 4 {
		return false
	}
	for i := 0; i < rows.Len(); i++ {
		row := reflect.Indirect(rows.Index(i))
		if row.FieldByName("Label").IsNil() == selected || !row.FieldByName("Unused").IsNil() {
			return false
		}
	}
	return true
}

func TestRelationSelectionDoesNotInheritAcrossRuntimeEntry(t *testing.T) {
	runtime, target, db, inputType, view := relationSelectionFixture(t)
	_, err := db.Exec("DROP TABLE unused")
	require.NoError(t, err)
	for _, foreign := range []bool{false, true} {
		owner := target
		if foreign {
			owner.Component.Scope = "example.com/unrelated"
			owner.Route.Path = "/other"
		}
		ctx, err := exec.EnterRelationSelection(context.Background(), owner, exec.NewRelationSelection(owner, view))
		require.NoError(t, err)
		_, err = runtime.InvokeComponent(ctx, exec.ComponentRequest{Target: target, Input: reflect.New(inputType).Interface()})
		require.ErrorContains(t, err, "unused", "nested entry must clear prior empty relation selection")
	}
	foreign := target
	foreign.Route.Path = "/foreign"
	_, err = runtime.InvokeComponent(context.Background(), exec.ComponentRequest{Target: target, Input: reflect.New(inputType).Interface(), ReaderOptions: &exec.ReaderOptions{Relations: exec.NewRelationSelection(foreign, view)}})
	require.ErrorContains(t, err, "relation selection does not match target")
}
