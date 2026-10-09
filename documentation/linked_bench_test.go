package docs

import (
	"context"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func BenchmarkLinkedDocumentation(b *testing.B) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	snapshot, err := (Loader{}).Load(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "rows", Source: &spec.ViewSource{SQL: "SELECT u.id AS id FROM users u"}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
	component.RootView.CompileProjectionOrigins()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := snapshot.ForComponent(component, reflect.TypeFor[output]()); err != nil {
			b.Fatal(err)
		}
	}
}
