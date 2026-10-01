package writer

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func TestWriterInsertPresenceOnlyInitialPass(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
		component.RootView.InsertValidationPresence = enabled
		handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
		if err != nil {
			t.Fatal(err)
		}
		input := &sqPlainInput{Rows: []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true}}}}
		snapshot, err := handler.CaptureInput(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		caps := &fakeCapabilities{}
		if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps}); err != nil {
			t.Fatal(err)
		}
		if len(caps.validations) != 2 {
			t.Fatalf("passes%v", caps.validations)
		}
		first, last := caps.validations[0][0], caps.validations[1][0]
		for _, policy := range []xhandler.ValidationOptions{first, last} {
			if policy.CheckUnique == nil || !*policy.CheckUnique || policy.CheckRef == nil || !*policy.CheckRef {
				t.Fatalf("mandatory database checks missing %+v", policy)
			}
		}
		if first.Action != xhandler.WriteInsert || first.Previous != nil || first.HonorPresence != enabled {
			t.Fatalf("first %+v", first)
		}
		if enabled && (first.Fields == nil || !first.Fields.Has("Name") || first.Fields.Has("ID")) {
			t.Fatal("genuine Has coverage lost")
		}
		if !enabled && first.Fields != nil {
			t.Fatal("default insert became sparse")
		}
		if last.HonorPresence || last.Fields != nil || last.Previous != nil {
			t.Fatalf("final not complete %+v", last)
		}
	}
}

type insertRequiredHas struct{ ID, Name bool }
type insertRequiredRow struct {
	ID   int                `sqlx:"id,primaryKey"`
	Name *string            `sqlx:"name,required"`
	Has  *insertRequiredHas `setMarker:"true" sqlx:"-"`
}
type insertRequiredInput struct {
	Rows []*insertRequiredRow `parameter:"Rows,kind=body,in=Data" view:"Rows,table=required_rows"`
}
type insertRequiredOutput struct{ Data []*insertRequiredRow }

func TestWriterPresenceCannotSkipFinalRequiredOrDatabaseConstraint(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := sqlite.New(t)
		if _, err := h.DB.Exec("CREATE TABLE required_rows(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE)"); err != nil {
			t.Fatal(err)
		}
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[insertRequiredInput]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", InsertValidationPresence: enabled, Source: &spec.ViewSource{Table: "required_rows"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
		input := &insertRequiredInput{Rows: []*insertRequiredRow{{ID: 1, Has: &insertRequiredHas{ID: true}}}}
		if _, err := runSQLiteWriter(t, context.Background(), h, component, input, &insertRequiredOutput{}, "post"); err == nil {
			t.Fatal("required final value accepted")
		}
		var count int
		if err := h.DB.QueryRow("SELECT COUNT(*) FROM required_rows").Scan(&count); err != nil || count != 0 {
			t.Fatalf("count%d err%v", count, err)
		}
		// The schema's UNIQUE constraint remains effective even without a validation tag.
		input.Rows[0].Name = ptr("taken")
		input.Rows[0].Has.Name = true
		if _, err := h.DB.Exec("INSERT INTO required_rows(id,name) VALUES(2,'taken')"); err != nil {
			t.Fatal(err)
		}
		if _, err := runSQLiteWriter(t, context.Background(), h, component, input, &insertRequiredOutput{}, "post"); err == nil {
			t.Fatal("database uniqueness accepted")
		}
		if err := h.DB.QueryRow("SELECT COUNT(*) FROM required_rows").Scan(&count); err != nil || count != 1 {
			t.Fatalf("count%d err%v", count, err)
		}
	}
}
