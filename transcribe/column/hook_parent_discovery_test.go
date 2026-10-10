package column

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"testing"
)

func TestHookParentKeyDiscoveryPreservesLogicalAuthority(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records (ID INTEGER, PARENT_ID INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, tag string
		parent    bool
		fail      bool
	}{
		{"hook parent", `sqlx:"-" relationKey:"hook"`, true, false},
		{"transient without hook", `sqlx:"-"`, true, true},
		{"invalid policy", `sqlx:"-" relationKey:"other"`, true, true},
		{"physical field", `relationKey:"hook"`, true, true},
		{"unrelated hook field", `sqlx:"-" relationKey:"hook"`, false, true},
		{"invariant still physical", `sqlx:"-" relationKey:"hook" invariant:"Owner"`, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const sql = `SELECT ID FROM (SELECT ID FROM records) p`
			key := &spec.Column{Name: "DerivedIDs", Source: "DERIVED_IDS", Type: spec.TypeRef{Name: "int", Cardinality: spec.CardinalityMany}, ExplicitType: true, Tag: test.tag}
			child := &spec.View{Name: "Children", Namespace: "c", Source: &spec.ViewSource{SQL: `SELECT PARENT_ID FROM records`}}
			link := &spec.RelationLink{ParentNamespace: "p", ParentColumn: "DERIVED_IDS", ParentField: "DerivedIDs", ChildColumn: "PARENT_ID"}
			if !test.parent {
				link.ParentColumn = "ID"
				link.ParentField = "ID"
			}
			view := &spec.View{Name: "Parents", Namespace: "p", Source: &spec.ViewSource{SQL: sql}, Columns: []*spec.Column{key}, Relations: []*spec.Relation{{Name: "Children", View: child, On: []*spec.RelationLink{link}}}}
			component := &spec.Component{RootView: view, Settings: &spec.Settings{DefaultConnector: "main"}}
			staticErr := New(nil).ValidateSourceProjections(component, nil)
			err := New(Connections{"main": h.DB}).Refine(context.Background(), component, nil, nil)
			if test.fail {
				if err == nil || staticErr == nil {
					t.Fatalf("missing physical projection accepted: static=%v discovery=%v", staticErr, err)
				}
				return
			}
			if staticErr != nil || err != nil {
				t.Fatalf("logical hook key rejected: static=%v discovery=%v", staticErr, err)
			}
			if view.Source.SQL != sql || len(view.Columns) != 2 || view.Columns[0].Name != "DerivedIDs" || view.Columns[0].Type != key.Type || view.Columns[0].Tag != key.Tag || view.Columns[0].DatabaseType != "" {
				t.Fatalf("logical authority or SQL changed: %+v", view)
			}
		})
	}
}

func TestHookParentDiscoveryStillRequiresChildProjection(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records (ID INTEGER)`); err != nil {
		t.Fatal(err)
	}
	child := &spec.View{Name: "Children", Namespace: "c", Source: &spec.ViewSource{SQL: `SELECT ID FROM records`}, Columns: []*spec.Column{{Name: "Missing", Source: "MISSING", ExplicitType: true, Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-" relationKey:"hook"`}}}
	parent := &spec.View{Name: "Parents", Namespace: "p", Source: &spec.ViewSource{SQL: `SELECT ID FROM records`}, Columns: []*spec.Column{{Name: "DerivedIDs", Source: "DERIVED_IDS", ExplicitType: true, Type: spec.TypeRef{Name: "int", Cardinality: spec.CardinalityMany}, Tag: `sqlx:"-" relationKey:"hook"`}}, Relations: []*spec.Relation{{Name: "Children", View: child, On: []*spec.RelationLink{{ParentNamespace: "p", ParentField: "DerivedIDs", ParentColumn: "DERIVED_IDS", ChildNamespace: "c", ChildField: "Missing", ChildColumn: "MISSING"}}}}}
	component := &spec.Component{RootView: parent, Settings: &spec.Settings{DefaultConnector: "main"}}
	if err := New(nil).ValidateSourceProjections(component, nil); err == nil {
		t.Fatal("child's hook tag hid missing SQL predicate output")
	}
	if err := New(Connections{"main": h.DB}).Refine(context.Background(), component, nil, nil); err == nil {
		t.Fatal("child's hook tag hid missing SQL predicate output")
	}
}
