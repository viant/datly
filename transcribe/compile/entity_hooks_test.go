package compile

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestEntityHooksViewDirective(t *testing.T) {
	for _, reference := range []string{"app.Hooks", "example.com/private/app.Hooks"} {
		t.Run(reference, func(t *testing.T) {
			original := &spec.View{Name: "Rows", Source: &spec.ViewSource{}}
			SQL := "SELECT r.*, c.*, entity_hooks(r, '" + reference + "'), entity_hooks(c, 'app.ChildHooks') FROM rows r JOIN children c ON c.parent_id=r.id"
			actual, err := NewReader().Compile(ReadInput{View: original, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			if original.EntityHooks != "" || actual.EntityHooks != reference || len(actual.Relations) != 1 || actual.Relations[0].View.EntityHooks != "app.ChildHooks" {
				t.Fatalf("root=%+v relations=%+v", actual, actual.Relations)
			}
			if strings.Contains(actual.Source.SQL, "entity_hooks") {
				t.Fatalf("generation directive leaked into SQL: %s", actual.Source.SQL)
			}
			cloned := actual.Clone()
			if cloned.EntityHooks != reference || cloned.Relations[0].View.EntityHooks != "app.ChildHooks" {
				t.Fatal("clone lost hook reference")
			}
		})
	}
}

func TestEntityHooksViewDirectiveErrors(t *testing.T) {
	for _, test := range []struct{ SQL, existing string }{
		{"SELECT r.*, entity_hooks(missing,'app.Hooks') FROM rows r", ""},
		{"SELECT r.*, entity_hooks(r,'') FROM rows r", ""},
		{"SELECT r.*, entity_hooks(r,app.Hooks) FROM rows r", ""},
		{"SELECT r.*, entity_hooks(r,true) FROM rows r", ""},
		{"SELECT r.*, entity_hooks(r,'a.Hooks'),entity_hooks(r,'b.Hooks') FROM rows r", ""},
		{"SELECT r.* FROM rows r WHERE entity_hooks(r,'a.Hooks')", ""},
		{"SELECT r.*, entity_hooks(r,'a.Hooks') FROM rows r", "b.Hooks"},
	} {
		t.Run(test.SQL+test.existing, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", EntityHooks: test.existing, Source: &spec.ViewSource{}}, SQL: test.SQL})
			if err == nil {
				t.Fatalf("unsupported directive succeeded: %s", test.SQL)
			}
		})
	}
}
