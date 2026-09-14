package docs_test

import (
	"context"
	"github.com/viant/bindly/resource"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
	"reflect"
	"testing"
	"testing/fstest"
)

type lineageRow struct {
	ID int `sqlx:"id" json:"id"`
}
type lineageOutput struct {
	Rows []lineageRow `parameter:"Rows,kind=output,in=view"`
}

func TestDictionaryDoesNotAttributeWildcardDuplicateToTable(t *testing.T) {
	store := resource.New()
	if err := store.Register("p", fstest.MapFS{"docs.yaml": {Data: []byte("Columns:\n  users: {id: User ID}\n  orders: {id: Order ID}\nPaths:\n  Rows.ID: Row identifier\n")}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (docs.Loader{Resources: store}).Load(context.Background(), xdocs.Source{DocURL: "p:docs.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ sql, want string }{
		{"SELECT u.*,o.id AS id FROM users u JOIN orders o ON o.user_id=u.id", "Row identifier"},
		{"SELECT o.id AS id,u.* FROM users u JOIN orders o ON o.user_id=u.id", "Row identifier"},
		{"SELECT o.id AS id FROM users u JOIN orders o ON o.user_id=u.id", "Order ID"},
	} {
		component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "rows", Source: &spec.ViewSource{SQL: test.sql}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
		scoped, err := snapshot.ForComponent(component, reflect.TypeFor[lineageOutput]())
		if err != nil {
			t.Fatal(err)
		}
		annotation := scoped.StructField("Rows.ID", reflect.TypeFor[lineageRow]().Field(0))
		if annotation.Description != test.want {
			t.Fatalf("%s: %q", test.sql, annotation.Description)
		}
	}
}
