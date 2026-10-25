package report

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCubeLowerCamelPreservesOriginalAcronymRule(t *testing.T) {
	type useCase struct{ source, want string }
	for _, tc := range []useCase{{"", ""}, {"VStart", "vStart"}, {"V100", "v100"}, {"ViewConversions", "viewConversions"}, {"VStartID", "vStartId"}, {"IsPG", "isPg"}, {"AccountID", "accountId"}, {"URL", "url"}, {"EventDate", "eventDate"}, {"Video25", "video25"}, {"AccountIDs", "accountIdS"}} {
		if got := lowerCamel(tc.source); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.source, got, tc.want)
		}
	}
}

func TestCubeAndComposeAcronymFilterRetainSourceIdentity(t *testing.T) {
	type sourceInput struct{ IsPG bool }
	source := reportSource(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true}})
	parameter := &spec.Parameter{Name: "IsPG", Source: spec.BindSource{Kind: "query", Name: "is_pg"}}
	source.Component.Parameters = []*spec.Parameter{parameter}
	source.OutputType = reflect.TypeFor[struct{ IsPG bool }]()
	source.Component.RootView.Relations = nil
	source.Component.RootView.Columns = []*spec.Column{{Name: "IsPG", Source: "is_pg", Groupable: boolPointer(true)}}
	for _, describe := range []bool{false, true} {
		engine := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[sourceInput]()})
		var compiled *handlercompiler.Result
		var err error
		if describe {
			compiled, err = engine.Describe()
		} else {
			compiled, err = engine.Compile()
		}
		if err != nil {
			t.Fatal(err)
		}
		source.Input = compiled.Input
		if compiled.Bindings[0].Name != "IsPG" || compiled.Bindings[0].Location.In != "is_pg" || compiled.Bindings[0].Extension != parameter {
			t.Fatal("ordinary source identity changed")
		}
		project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
		if err != nil {
			t.Fatal(err)
		}
		if len(project.Derived()) != 2 {
			t.Fatal("cube and compose required")
		}
		for _, derived := range project.Derived() {
			owner := derived.InputType
			compose := strings.HasSuffix(derived.Component.Routes[0].Path, "/compose")
			if compose {
				cubes, _ := owner.FieldByName("Cubes")
				owner = cubes.Type.Elem()
			}
			filters, _ := owner.FieldByName("Filters")
			field, ok := filters.Type.FieldByName("IsPG")
			if !ok || strings.Split(field.Tag.Get("json"), ",")[0] != "isPg" {
				t.Fatalf("wrong filter identity: %v %s", ok, field.Tag)
			}
			value := reflect.New(derived.InputType)
			body := `{"dimensions":{"isPg":true},"filters":{"isPg":false}}`
			if compose {
				body = `{"cubes":[{"filters":{"isPg":false}}]}`
			}
			if err := json.Unmarshal([]byte(body), value.Interface()); err != nil {
				t.Fatal(err)
			}
			frame := value.Elem()
			if compose {
				frame = frame.FieldByName("Cubes").Index(0)
			}
			actual := frame.FieldByName("Filters").FieldByName("IsPG")
			if actual.IsNil() || actual.Elem().Bool() {
				t.Fatal("explicit false filter lost")
			}
			if !compose {
				encoded, err := json.Marshal(derived.Definition)
				if err != nil {
					t.Fatal(err)
				}
				var persisted Definition
				if err = json.Unmarshal(encoded, &persisted); err != nil {
					t.Fatal(err)
				}
				rebuilt, err := persisted.Compile(derived.InputType, derived.OutputType)
				if err != nil {
					t.Fatal(err)
				}
				if len(rebuilt.filters) != 1 || rebuilt.filters[0].name != "IsPG" || rebuilt.filters[0].location.In != "is_pg" {
					t.Fatal("persisted source mapping changed")
				}
				fields, err := rebuilt.selectedFields(value.Elem())
				if err != nil || !reflect.DeepEqual(fields, []string{"IsPG"}) {
					t.Fatalf("selection lost: %v %v", fields, err)
				}
			}
		}
	}
}
