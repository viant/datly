package generate

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"
)

func TestNilSlicePolicyGeneratedMetadataStable(t *testing.T) {
	for _, policy := range []string{"empty_array", "null"} {
		t.Run(policy, func(t *testing.T) {
			component := &spec.Component{Name: "Records", Routes: []*spec.Route{{Path: "/records", Method: "GET"}}, Settings: &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: policy}}, RootView: &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}}
			dir := t.TempDir()
			first, err := New(Input{Component: component}).Generate(dir)
			if err != nil {
				t.Fatal(err)
			}
			second, err := New(Input{Component: component}).Generate(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(first.Files, second.Files) {
				t.Fatal("second generation changed emitted artifacts")
			}
			found := false
			for _, file := range second.Files {
				parsed, err := parser.ParseFile(token.NewFileSet(), file.Path, file.Content, 0)
				if err != nil {
					t.Fatal(err)
				}
				ast.Inspect(parsed, func(node ast.Node) bool {
					field, ok := node.(*ast.Field)
					if !ok || field.Tag == nil {
						return true
					}
					raw, err := strconv.Unquote(field.Tag.Value)
					if err != nil {
						t.Fatal(err)
					}
					tag := reflect.StructTag(raw)
					if _, ok := tag.Lookup(dtag.OutputSettingsTag); !ok {
						return true
					}
					settings, err := dtag.ParseSettings(tag)
					if err != nil {
						t.Fatal(err)
					}
					var loaded spec.Settings
					settings.Apply(&loaded)
					if loaded.Output == nil || loaded.Output.NilSlicePolicy != policy {
						t.Fatalf("generated descriptor lost policy: %s", raw)
					}
					found = true
					return true
				})
			}
			if !found {
				t.Fatal("no generated output descriptor settings")
			}
		})
	}
}
