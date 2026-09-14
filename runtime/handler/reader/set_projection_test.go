package reader

import (
	"context"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/viant/afs/option"
	"github.com/viant/assertly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	xstate "github.com/viant/xdatly/state"
)

type setProjectionChild struct {
	ID       int
	ParentID int
	Name     string
}

type setProjectionParent struct {
	ID       int
	Children []*setProjectionChild `view:"children" sql:"SELECT id, parent_id, name FROM set_child_current UNION ALL SELECT id, parent_id, name FROM set_child_archived" on:"ID:id=ParentID:parent_id"`
}

type setProjectionOutput struct {
	Data []*setProjectionParent
}

func TestService_ProjectsUnionRelationAndReplaysNativeCache_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE set_parent (id INTEGER);`,
		`CREATE TABLE set_child_current (id INTEGER, parent_id INTEGER, name TEXT);`,
		`CREATE TABLE set_child_archived (id INTEGER, parent_id INTEGER, name TEXT);`,
		`INSERT INTO set_parent(id) VALUES (1), (2);`,
		`INSERT INTO set_child_current(id, parent_id, name) VALUES (10, 1, 'current-one'), (20, 2, 'current-two');`,
		`INSERT INTO set_child_archived(id, parent_id, name) VALUES (30, 1, 'archived-one'), (40, 2, 'archived-two');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "SetProjection",
		RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{
			SQL: `SELECT id FROM set_parent ORDER BY id`,
		}, Selector: &spec.Selector{AllowFields: true}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(setProjectionOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	childView := artifact.Reader.Root.View.Relations[0].Of.View
	childView.Spec.Selector = &spec.Selector{AllowFields: true}
	readCache, err := cacheafs.NewCache("mem://localhost/"+t.Name()+"/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create SQLx cache: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(setProjectionOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}), ReadCaches: map[*data.View]cache.Cache{},
		Providers: selectorProviders(xstate.Selectors{
			&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"ID", "Children"}}},
			&xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Fields: []string{"Name"}}},
		}),
	}
	registerViewCaches(session, artifact.Reader.Root.View, readCache)

	want := &setProjectionOutput{Data: []*setProjectionParent{
		{ID: 1, Children: []*setProjectionChild{{ParentID: 1, Name: "current-one"}, {ParentID: 1, Name: "archived-one"}}},
		{ID: 2, Children: []*setProjectionChild{{ParentID: 2, Name: "current-two"}, {ParentID: 2, Name: "archived-two"}}},
	}}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cold set projection read: %v", err)
	}
	assertly.AssertValues(t, want, actual)
	if _, err := h.DB.Exec(`DELETE FROM set_parent; DELETE FROM set_child_current; DELETE FROM set_child_archived`); err != nil {
		t.Fatalf("delete set projection sources: %v", err)
	}
	actual, err = NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("native cache set projection replay: %v", err)
	}
	assertly.AssertValues(t, want, actual)
}
