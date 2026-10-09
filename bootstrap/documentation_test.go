package bootstrap

import (
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

type documentationRow struct {
	ID int `sqlx:"id" docTable:"wrong" docColumn:"wrong"`
}
type documentationOutput struct{ Rows []documentationRow }

func documentationArtifact(t testing.TB, sql string, resources *resource.Store) *Artifact {
	t.Helper()
	source := &spec.ViewSource{SQL: sql}
	if strings.Contains(sql, "${embed:p:rows.sql}") {
		source.Embeds = []*spec.EmbeddedSQLRef{{Path: "p:rows.sql", Raw: "${embed:p:rows.sql}"}}
	}
	if sql == "uri" {
		source.SQL = ""
		source.URI = "p:rows.sql"
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: &spec.Component{RootView: &spec.View{Name: "Rows", Source: source}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}, OutputType: reflect.TypeFor[documentationOutput](), Resources: resources, Documentation: xdocs.Source{DocURL: "p:docs.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}
func documentationResources(t testing.TB) *resource.Store {
	t.Helper()
	resources := resource.New()
	if err := resources.Register("p", fstest.MapFS{"docs.yaml": {Data: []byte("Columns:\n  users: {id: User ID}\n  orders: {id: Order ID}\n  sales.users: {id: Qualified ID}\nPaths:\n  Rows.ID: Unknown ID\n")}, "rows.sql": {Data: []byte("SELECT id FROM orders")}}); err != nil {
		t.Fatal(err)
	}
	return resources
}
func TestArtifactDocumentationOccurrences(t *testing.T) {
	resources := documentationResources(t)
	for _, tc := range []struct{ sql, want string }{
		{"SELECT id FROM users", "User ID"},
		{"SELECT u.id AS id FROM users u", "User ID"},
		{"WITH q AS (SELECT id FROM orders) SELECT id FROM q", "Order ID"},
		{"SELECT o.id AS id FROM users u JOIN orders o ON o.id=u.id", "Order ID"},
		{"SELECT id FROM sales.users", "Qualified ID"},
		{"SELECT COUNT(*) AS id FROM users", "Unknown ID"},
		{"SELECT u.*,o.id AS id FROM users u JOIN orders o ON o.id=u.id", "Unknown ID"},
		{"SELECT o.id AS id,u.* FROM users u JOIN orders o ON o.id=u.id", "Unknown ID"},
		{"SELECT id FROM (${embed:p:rows.sql}) q", "Order ID"},
		{"uri", "Order ID"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			artifact := documentationArtifact(t, tc.sql, resources)
			for i := 0; i < 3; i++ {
				annotation := artifact.Documentation.StructField("Rows.ID", reflect.TypeFor[documentationRow]().Field(0))
				if annotation.Description != tc.want {
					t.Fatalf("got %q, want %q", annotation.Description, tc.want)
				}
			}
		})
	}
}
func TestArtifactDocumentationRebuild(t *testing.T) {
	resources := documentationResources(t)
	first := documentationArtifact(t, "SELECT id FROM users", resources)
	second := documentationArtifact(t, "SELECT id FROM orders", resources)
	field := reflect.TypeFor[documentationRow]().Field(0)
	if first.Documentation.StructField("Rows.ID", field).Description != "User ID" || second.Documentation.StructField("Rows.ID", field).Description != "Order ID" {
		t.Fatal("reused row lost occurrence attribution")
	}
}
func BenchmarkDocumentationBootstrap(b *testing.B) {
	resources := documentationResources(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		documentationArtifact(b, "SELECT id FROM users", resources)
	}
}
func BenchmarkDocumentationProjection(b *testing.B) {
	artifact := documentationArtifact(b, "SELECT id FROM users", documentationResources(b))
	field := reflect.TypeFor[documentationRow]().Field(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		artifact.Documentation.StructField("Rows.ID", field)
	}
}

func TestArtifactDocumentationCanonicalSQLX(t *testing.T) {
	artifact := documentationArtifact(t, "SELECT id FROM users", documentationResources(t))
	for _, tc := range []struct{ tag, want string }{
		{`sqlx:"id|output_id,table=orders"`, "Order ID"},
		{`sqlx:"id|output_id,db=sales,table=users"`, "Qualified ID"},
	} {
		field := reflect.StructField{Name: "ID", Tag: reflect.StructTag(tc.tag)}
		if got := artifact.Documentation.StructField("Rows.ID", field).Description; got != tc.want {
			t.Fatalf("%s: %q", tc.tag, got)
		}
	}
}

func TestArtifactDocumentationSnapshotIsolation(t *testing.T) {
	artifact := documentationArtifact(t, "SELECT id FROM users", documentationResources(t))
	view := &artifact.Reader.Root.View.Spec
	view.Source.Table = "orders"
	view.Columns[0].Tag = `desc:"Changed by caller"`
	view.Columns[0].Source = "other_id"
	if got := artifact.Documentation.StructField("Rows.ID", reflect.TypeFor[documentationRow]().Field(0)).Description; got != "User ID" {
		t.Fatalf("snapshot changed with canonical metadata: %q", got)
	}
}

type documentationAliasRow struct {
	UserID int `sqlx:"id|user_id"`
}
type documentationControlledRow struct {
	UserID int `sqlx:"id|user_id" selectorAlias:"user_selector" sqlOutput:"user_id"`
}

func TestArtifactDocumentationPhysicalProjectionAliases(t *testing.T) {
	type aliasOutput struct{ Rows []documentationAliasRow }
	type controlledOutput struct{ Rows []documentationControlledRow }
	resources := documentationResources(t)
	for _, typ := range []reflect.Type{reflect.TypeFor[aliasOutput](), reflect.TypeFor[controlledOutput]()} {
		for _, tc := range []struct{ table, want string }{{"users", "User ID"}, {"orders", "Order ID"}} {
			sql := "SELECT id AS user_id FROM " + tc.table
			artifact, err := BuildArtifact(ArtifactInput{Component: &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}, OutputType: typ, Resources: resources, Documentation: xdocs.Source{DocURL: "p:docs.yaml"}})
			if err != nil {
				t.Fatal(err)
			}
			field := typ.Field(0).Type.Elem().Field(0)
			if got := artifact.Documentation.StructField("Rows.UserID", field).Description; got != tc.want {
				t.Fatalf("%v %s: got %q want %q", typ, sql, got, tc.want)
			}
			column := artifact.Reader.Root.View.Columns[0]
			if column.Column != "id" || column.Tag != string(field.Tag) {
				t.Fatalf("canonical mapping changed: %+v", column)
			}
			if artifact.Reader.Root.View.Spec.Source.SQL != sql {
				t.Fatal("runtime SQL changed")
			}
		}
	}
}
func TestArtifactDocumentationNestedLineage(t *testing.T) {
	type child struct {
		UserID int `sqlx:"id|user_id"`
	}
	type scalarChild struct {
		ID int `sqlx:"id"`
	}
	type parent struct {
		Totals     []scalarChild `view:"totals" sql:"SELECT COUNT(*) AS id FROM users" on:"ID:id=ID:id"`
		Duplicates []scalarChild `view:"duplicates" sql:"SELECT u.*,o.id AS id FROM users u JOIN orders o ON o.id=u.id" on:"ID:id=ID:id"`
		ID         int           `sqlx:"id"`
		Children   []child       `view:"children" sql:"SELECT o.id AS user_id FROM users u JOIN orders o ON o.id=u.id" on:"ID:id=UserID:id"`
	}
	type output struct{ Rows []parent }
	artifact, err := BuildArtifact(ArtifactInput{Component: &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT id FROM users"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}, OutputType: reflect.TypeFor[output](), Resources: documentationResources(t), Documentation: xdocs.Source{DocURL: "p:docs.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := artifact.Documentation.StructField("Rows.Children.UserID", reflect.TypeFor[child]().Field(0)).Description; got != "Order ID" {
		t.Fatalf("nested attribution: %q", got)
	}
	for _, path := range []string{"Rows.Totals.ID", "Rows.Duplicates.ID"} {
		if got := artifact.Documentation.StructField(path, reflect.TypeFor[scalarChild]().Field(0)).Description; got != "" {
			t.Fatalf("unknown nested origin inherited a table description at %s: %q", path, got)
		}
	}
}

func TestArtifactDocumentationUnknownKeepsGlobalColumn(t *testing.T) {
	resources := resource.New()
	if err := resources.Register("p", fstest.MapFS{"docs.yaml": {Data: []byte("Columns:\n  id: Generic identifier\n  users: {id: Physical user identifier}\nPaths:\n  Rows.ID: Path identifier\n")}}); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"SELECT COUNT(*) AS id FROM users", "SELECT u.*,o.id AS id FROM users u JOIN orders o ON o.id=u.id"} {
		artifact := documentationArtifact(t, sql, resources)
		if got := artifact.Documentation.StructField("Rows.ID", reflect.TypeFor[documentationRow]().Field(0)).Description; got != "Generic identifier" {
			t.Fatalf("%s: %q", sql, got)
		}
	}
}

func TestArtifactDocumentationOutputLabelDoesNotOverwriteCanonicalName(t *testing.T) {
	type row struct {
		ID    int `sqlx:"id" sqlOutput:"owner_id"`
		Other int `sqlx:"other_id" sqlOutput:"id"`
	}
	type output struct{ Rows []row }
	resources := resource.New()
	if err := resources.Register("p", fstest.MapFS{"docs.yaml": {Data: []byte("Columns:\n  users: {id: Owner identifier}\n  orders: {other_id: Other identifier}\n")}}); err != nil {
		t.Fatal(err)
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT u.id AS owner_id,o.other_id AS id FROM users u JOIN orders o ON o.other_id=u.id"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}, OutputType: reflect.TypeFor[output](), Resources: resources, Documentation: xdocs.Source{DocURL: "p:docs.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"Owner identifier", "Other identifier"} {
		field := reflect.TypeFor[row]().Field(i)
		if got := artifact.Documentation.StructField("Rows."+field.Name, field).Description; got != want {
			t.Fatalf("%s: got %q want %q", field.Name, got, want)
		}
	}
}
