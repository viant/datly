package column

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"testing"
)

func TestComputedFieldDiscoveryPreservesGoAuthority(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records (ID INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, tag                       string
		deleteMarker, concurrency, fail bool
	}{
		{name: "computed map", tag: `sqlx:"-"`},
		{name: "physical CAST", fail: true},
		{name: "invariant", tag: `sqlx:"-" invariant:"Owner"`, fail: true},
		{name: "delete marker", tag: `sqlx:"-"`, deleteMarker: true, fail: true},
		{name: "concurrency token", tag: `sqlx:"-"`, concurrency: true, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column := &spec.Column{Name: "CreativeSpendIndex", Source: "CREATIVE_SPEND_INDEX", ExplicitType: true, Type: spec.TypeRef{Name: "map[int]float64"}, Tag: tc.tag, DeleteMarker: tc.deleteMarker, ConcurrencyToken: tc.concurrency}
			const SQL = `SELECT ID FROM records`
			view := &spec.View{Name: "Audience", Namespace: "audience", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{column}}
			component := &spec.Component{RootView: view, Settings: &spec.Settings{DefaultConnector: "main"}}
			authoredErr := ValidateProjectionAnnotations(view, []string{"ID"})
			staticErr := New(nil).ValidateSourceProjections(component, nil)
			discoveryErr := New(Connections{"main": h.DB}).Refine(context.Background(), component, nil, nil)
			if tc.fail {
				if authoredErr == nil || staticErr == nil || discoveryErr == nil {
					t.Fatalf("missing physical field accepted: authored=%v static=%v discovery=%v", authoredErr, staticErr, discoveryErr)
				}
				return
			}
			if authoredErr != nil || staticErr != nil || discoveryErr != nil {
				t.Fatalf("computed field rejected: authored=%v static=%v discovery=%v", authoredErr, staticErr, discoveryErr)
			}
			if view.Source.SQL != SQL || len(view.Columns) != 2 || view.Columns[0].Name != column.Name || view.Columns[0].Type != column.Type || view.Columns[0].Tag != column.Tag || column.Type.Name != "map[int]float64" || column.Tag != tc.tag || column.DatabaseType != "" {
				t.Fatalf("computed field or SQL changed: %+v", view)
			}
		})
	}
}

func TestComputedMapStillRequiresPhysicalRelationKey(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records (ID INTEGER)`); err != nil {
		t.Fatal(err)
	}
	key := &spec.Column{Name: "Index", Source: "INDEX_KEY", ExplicitType: true, Type: spec.TypeRef{Name: "map[int]float64"}, Tag: `sqlx:"-"`}
	child := &spec.View{Name: "Children", Namespace: "c", Source: &spec.ViewSource{SQL: `SELECT ID FROM records`}}
	parent := &spec.View{Name: "Parents", Namespace: "p", Source: &spec.ViewSource{SQL: `SELECT ID FROM records`}, Columns: []*spec.Column{key}, Relations: []*spec.Relation{{Name: "Children", View: child, On: []*spec.RelationLink{{ParentNamespace: "p", ParentField: "Index", ParentColumn: "INDEX_KEY", ChildColumn: "ID"}}}}}
	component := &spec.Component{RootView: parent, Settings: &spec.Settings{DefaultConnector: "main"}}
	if err := New(nil).ValidateSourceProjections(component, nil); err == nil {
		t.Fatal("computed map hid a missing parent relation key")
	}
	if err := New(Connections{"main": h.DB}).Refine(context.Background(), component, nil, nil); err == nil {
		t.Fatal("computed map hid a missing parent relation key in discovery")
	}
}
