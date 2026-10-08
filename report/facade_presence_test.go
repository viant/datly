package report

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestFacadeProxiesOrdinaryInputsAndPredicatePresence(t *testing.T) {
	type payload struct {
		Enabled bool `json:"enabled"`
	}
	type inputHas struct {
		Active   bool
		Label    bool
		Criteria bool
		Internal bool
		Header   bool
		ID       bool
		Tags     bool
		Payload  bool
	}
	type input struct {
		Criteria string
		Active   bool
		Label    string
		Internal string `internal:"true"`
		Header   string
		ID       int
		Tags     []string
		Payload  payload
		Has      *inputHas `setMarker:"true" json:"-"`
	}
	groupable := true
	source := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/source", Name: "Rows"}, Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/rows"}}, RootView: &spec.View{Name: "rows", Groupable: &groupable, Source: &spec.ViewSource{Table: "rows"}, Columns: []*spec.Column{{Name: "ID", Source: "id", Groupable: &groupable}}}, Parameters: []*spec.Parameter{
		{Name: "Active", Source: spec.BindSource{Kind: "query", Name: "active"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"rows", "active"}}}},
		{Name: "Criteria", Source: spec.BindSource{Kind: "query", Name: "criteria"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyCriteria}},
		{Name: "Label", Source: spec.BindSource{Kind: "query", Name: "label"}},
		{Name: "Internal", Source: spec.BindSource{Kind: "query", Name: "internal"}},
		{Name: "Header", Source: spec.BindSource{Kind: "header", Name: "X-Label"}},
		{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
		{Name: "Tags", Source: spec.BindSource{Kind: "form", Name: "tags"}},
		{Name: "Payload", Source: spec.BindSource{Kind: "body", Name: "payload"}},
	}}
	compiled, err := handlercompiler.New(handlercompiler.Input{Component: source, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{{Component: source, Input: compiled.Input, OutputType: reflect.TypeFor[struct{}]()}})
	if err != nil {
		t.Fatal(err)
	}
	cube := project.Derived()[0]
	filters, _ := cube.InputType.FieldByName("Filters")
	if _, ok := filters.Type.FieldByName("Label"); !ok {
		t.Fatal("non-predicate input missing")
	}
	if _, ok := filters.Type.FieldByName("Criteria"); !ok {
		t.Fatal("source criteria input missing")
	}
	if _, ok := filters.Type.FieldByName("Internal"); ok {
		t.Fatal("internal input exposed")
	}
	marker, ok := filters.Type.FieldByName("Has")
	if !ok || marker.Tag.Get("setMarker") != "true" || marker.Tag.Get("json") != "-" {
		t.Fatal("predicate presence marker missing or public")
	}
	value := reflect.New(cube.InputType)
	section := value.Elem().FieldByName("Filters")
	presence := reflect.New(marker.Type.Elem())
	presence.Elem().FieldByName("Active").SetBool(true)
	presence.Elem().FieldByName("Label").SetBool(true)
	for _, name := range []string{"Header", "ID", "Tags", "Payload"} {
		presence.Elem().FieldByName(name).SetBool(true)
	}
	section.FieldByName("Has").Set(presence)
	active := false
	section.FieldByName("Active").Set(reflect.ValueOf(&active))
	label := ""
	section.FieldByName("Label").Set(reflect.ValueOf(&label))
	header, id, tags, body := "header-value", 0, []string{}, payload{Enabled: false}
	section.FieldByName("Header").Set(reflect.ValueOf(&header))
	section.FieldByName("ID").Set(reflect.ValueOf(&id))
	section.FieldByName("Tags").Set(reflect.ValueOf(&tags))
	section.FieldByName("Payload").Set(reflect.ValueOf(&body))
	providers, err := cube.Plan.providers(value.Elem(), nil)
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(providers...))
	if err != nil {
		t.Fatal(err)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/rows"})
	out := &input{Active: true}
	if err := injector.Bind(context.Background(), out, bindly.WithPlan(route.Plan())); err != nil {
		t.Fatal(err)
	}
	if out.Active || out.Has == nil || !out.Has.Active || !out.Has.Label || out.Has.Criteria {
		t.Fatalf("source presence/value lost: %+v", out)
	}
	if out.Header != header || out.ID != id || out.Tags == nil || len(out.Tags) != 0 || out.Payload.Enabled || !out.Has.Header || !out.Has.ID || !out.Has.Tags || !out.Has.Payload {
		t.Fatalf("original header/path/form/body contract mapping or presence lost: %+v", out)
	}
}
