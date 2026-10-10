package compile

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"strings"
	"testing"
)

func TestHookParentKeyIsNotSQLProjection(t *testing.T) {
	type parent struct {
		ID         int
		DerivedIDs []int `sqlx:"-" relationKey:"hook"`
	}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(parent{}), x.WithName("Parent"), x.WithPkgPath("example.com/hooks"))); err != nil {
		t.Fatal(err)
	}
	types, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/hooks"})
	if err != nil {
		t.Fatal(err)
	}
	const source = `SELECT p.*, c.*, type(p,'Parent')
FROM (SELECT 1 AS ID) p
JOIN (SELECT 1 AS PARENT_ID) c ON p.DERIVED_IDS=c.PARENT_ID`
	view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "parents"}, SQL: source, Types: types})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(view.Source.SQL, "DERIVED_IDS") {
		t.Fatalf("hook key was projected into SQL: %s", view.Source.SQL)
	}
	if len(view.Columns) != 1 || view.Columns[0].Type.Name != "int" || view.Columns[0].Type.Cardinality != spec.CardinalityMany {
		t.Fatalf("linked hook type lost: %+v", view.Columns)
	}
	component := &spec.Component{RootView: view}
	if err = BackfillRelationMetadata(component); err != nil {
		t.Fatal(err)
	}
	if len(view.Relations) != 1 || len(view.Relations[0].On) != 1 {
		t.Fatalf("relations: %+v", view.Relations)
	}
	if view.Relations[0].On[0].ParentField != "DerivedIDs" {
		t.Fatalf("hook field lost: %+v", view.Relations[0].On[0])
	}
	if err = BackfillRelationMetadata(component); err != nil {
		t.Fatal(err)
	}
}

func TestHookRelationMetadataDoesNotHideMissingPhysicalKeys(t *testing.T) {
	for _, test := range []struct{ name, metadata, join string }{
		{"ordinary parent", "", "p.MISSING=c.PARENT_ID"},
		{"transient without hook", `, CAST(p.MISSING AS []int), tag(p.MISSING,'sqlx:"-"')`, "p.MISSING=c.PARENT_ID"},
		{"invalid hook policy", `, CAST(p.MISSING AS []int), tag(p.MISSING,'sqlx:"-" relationKey:"other"')`, "p.MISSING=c.PARENT_ID"},
		{"child predicate remains physical", `, CAST(c.MISSING AS []int), tag(c.MISSING,'sqlx:"-" relationKey:"hook"')`, "p.ID=c.MISSING"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := `SELECT p.*, c.*` + test.metadata + ` FROM (SELECT 1 AS ID) p JOIN (SELECT 1 AS PARENT_ID) c ON ` + test.join
			if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "parents"}, SQL: source}); err == nil {
				t.Fatal("missing SQL key accepted")
			}
		})
	}
}

func TestHookMetadataOnlySkipsParentProjectionProof(t *testing.T) {
	view := &data.View{Spec: spec.View{Namespace: "p", Source: &spec.ViewSource{SQL: "SELECT ID FROM (SELECT 1 AS ID) p"}}, Columns: []*data.Column{{Name: "DerivedIDs", Column: "DERIVED_IDS", Tag: `sqlx:"-" relationKey:"hook"`}}}
	link := data.NewLink("p", "DERIVED_IDS", "DerivedIDs")
	if err := resolveProjectedLinks(view, data.Links{link}, false); err != nil {
		t.Fatal(err)
	}
	if err := resolveProjectedLinks(view, data.Links{link}, true); err == nil {
		t.Fatal("hook tag hid a missing child SQL predicate key")
	}
}
