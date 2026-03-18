package openapi

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	openapi3 "github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format"
	"github.com/viant/xreflect"
)

type fakeDocService struct {
	lookup func(key string) (string, bool, error)
}

func (f *fakeDocService) Lookup(ctx context.Context, key string) (string, bool, error) {
	if f.lookup == nil {
		return "", false, nil
	}
	return f.lookup(key)
}

func setUnexportedField(target interface{}, fieldName string, value interface{}) {
	v := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func newTestComponent(t *testing.T) *repository.Component {
	t.Helper()
	component, err := repository.NewComponent(&contract.Path{Method: "POST", URI: "/v1/test"}, repository.WithView(&view.View{Template: &view.Template{}, Selector: &view.Config{}}))
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}
	types := xreflect.NewTypes()
	setUnexportedField(component, "types", types)
	return component
}

func TestPathsBuilderAddPath(t *testing.T) {
	builder := &PathsBuilder{paths: openapi3.Paths{}}
	item := &openapi3.PathItem{Summary: "sum"}
	builder.AddPath("/v1/pets", item)
	if builder.paths["/v1/pets"] != item {
		t.Fatalf("path not added")
	}
}

func TestGeneratorHelpers_Table(t *testing.T) {
	t.Run("forEachParam recursive and error", func(t *testing.T) {
		g := &generator{}
		called := 0
		params := state.Parameters{
			{Name: "root", Object: state.Parameters{{Name: "child1"}}, Repeated: state.Parameters{{Name: "child2"}}},
		}
		err := g.forEachParam(params, func(parameter *state.Parameter) (bool, error) {
			called++
			if parameter.Name == "child1" {
				return true, errors.New("boom")
			}
			return true, nil
		})
		if err == nil || err.Error() != "boom" {
			t.Fatalf("expected boom, got %v", err)
		}
		if called < 2 {
			t.Fatalf("expected recursive traversal")
		}
	})

	t.Run("index parameters", func(t *testing.T) {
		g := &generator{}
		params := []*openapi3.Parameter{{Name: "a"}, {Name: "b"}}
		indexed := g.indexParameters(params)
		if indexed["a"].Name != "a" || indexed["b"].Name != "b" {
			t.Fatalf("unexpected indexed values")
		}
	})

	t.Run("string ptr", func(t *testing.T) {
		if *stringPtr("x") != "x" {
			t.Fatalf("unexpected value")
		}
	})
}

func TestComponentSchemaHelpers_Table(t *testing.T) {
	component := newTestComponent(t)
	componentSchema := &ComponentSchema{component: component, schemas: NewContainer()}

	t.Run("isRequired", func(t *testing.T) {
		req := true
		in := contract.Input{Body: state.Type{Parameters: state.Parameters{{Required: &req}}}}
		if !componentSchema.isRequired(in) {
			t.Fatalf("expected required")
		}
	})

	t.Run("description and example defaults", func(t *testing.T) {
		desc, err := componentSchema.Description(context.Background(), "A", "default-desc")
		if err != nil || desc != "default-desc" {
			t.Fatalf("unexpected result: %q %v", desc, err)
		}
		example, err := componentSchema.Example(context.Background(), "A", "default-ex")
		if err != nil || example != "default-ex" {
			t.Fatalf("unexpected result: %q %v", example, err)
		}
	})

	t.Run("description and example from doc", func(t *testing.T) {
		componentSchema.doc = &fakeDocService{lookup: func(key string) (string, bool, error) {
			switch key {
			case "A":
				return "desc", true, nil
			case "A$example":
				return "ex", true, nil
			default:
				return "", false, nil
			}
		}}
		desc, err := componentSchema.Description(context.Background(), "A", "default-desc")
		if err != nil || desc != "desc" {
			t.Fatalf("unexpected description: %q %v", desc, err)
		}
		example, err := componentSchema.Example(context.Background(), "A", "default-ex")
		if err != nil || example != "ex" {
			t.Fatalf("unexpected example: %q %v", example, err)
		}
	})

	t.Run("description error", func(t *testing.T) {
		componentSchema.doc = &fakeDocService{lookup: func(key string) (string, bool, error) {
			return "", false, errors.New("lookup")
		}}
		if _, err := componentSchema.Description(context.Background(), "A", "default"); err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("typed/request/response schema", func(t *testing.T) {
		componentSchema.doc = nil
		component.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		component.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		component.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}

		reqSchema, err := componentSchema.RequestBody(context.Background())
		if err != nil || reqSchema == nil {
			t.Fatalf("unexpected request schema result: %v %v", reqSchema, err)
		}

		respSchema, err := componentSchema.ResponseBody(context.Background())
		if err != nil || respSchema == nil {
			t.Fatalf("unexpected response schema result: %v %v", respSchema, err)
		}

		if _, err = componentSchema.TypedSchema(context.Background(), component.Input.Type, "Input", component.IOConfig(), true); err != nil {
			t.Fatalf("unexpected typed schema error: %v", err)
		}
	})

	t.Run("type name and schema helpers", func(t *testing.T) {
		type sample struct{}
		types := xreflect.NewTypes()
		if err := types.Register("Sample", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(sample{}))); err != nil {
			t.Fatalf("register type failed: %v", err)
		}
		setUnexportedField(component, "types", types)

		if got := componentSchema.TypeName(reflect.TypeOf(sample{}), "fallback"); got != "Sample" {
			t.Fatalf("expected Sample, got %s", got)
		}

		refl := componentSchema.ReflectSchema("A", reflect.TypeOf(sample{}), "d", component.IOConfig())
		if refl == nil || refl.rType != reflect.TypeOf(sample{}) {
			t.Fatalf("unexpected reflect schema")
		}

		withTag := componentSchema.SchemaWithTag("F", reflect.TypeOf(sample{}), "d", component.IOConfig(), Tag{})
		if withTag == nil || withTag.path == "" {
			t.Fatalf("unexpected schema with tag")
		}
	})

	t.Run("schema with tag datatype override", func(t *testing.T) {
		type alt struct{ Value string }
		reg := xreflect.NewTypes()
		if err := reg.Register("Alt", xreflect.WithReflectType(reflect.TypeOf(alt{}))); err != nil {
			t.Fatalf("register type failed: %v", err)
		}
		if component.View.GetResource() == nil {
			component.View.SetResource(&view.Resource{})
		}
		component.View.GetResource().SetTypes(reg)
		withTag := componentSchema.SchemaWithTag("F", reflect.TypeOf(struct{ A int }{}), "d", component.IOConfig(), Tag{
			IsInput:   true,
			Parameter: &tags.Parameter{DataType: "Alt"},
		})
		if withTag.rType != reflect.TypeOf(alt{}) {
			t.Fatalf("expected datatype override")
		}
	})

	t.Run("schema with tag primitive datatype override", func(t *testing.T) {
		withTag := componentSchema.SchemaWithTag("Jwt", reflect.TypeOf(struct{ A int }{}), "d", component.IOConfig(), Tag{
			IsInput:   true,
			Parameter: &tags.Parameter{DataType: "string"},
		})
		if withTag.rType != reflect.TypeOf("") {
			t.Fatalf("expected primitive datatype override to string, got %v", withTag.rType)
		}
	})

	t.Run("schema with tag output keeps go type", func(t *testing.T) {
		goType := reflect.TypeOf(struct{ A int }{})
		withTag := componentSchema.SchemaWithTag("Out", goType, "d", component.IOConfig(), Tag{
			Parameter: &tags.Parameter{
				DataType: "string",
			},
		})
		if withTag.rType != goType {
			t.Fatalf("expected output kind to keep go type, got %v", withTag.rType)
		}
	})
}

func TestSchemaContainerCreateSchema_Table(t *testing.T) {
	container := NewContainer()
	componentSchema := &ComponentSchema{component: newTestComponent(t), schemas: container}
	fieldSchema := &Schema{path: "p", description: "d", rType: reflect.TypeOf(1)}

	t.Run("create primitive", func(t *testing.T) {
		result, err := container.CreateSchema(context.Background(), componentSchema, fieldSchema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Type != integerOutput {
			t.Fatalf("unexpected type: %s", result.Type)
		}
	})

	t.Run("get or generate delegates", func(t *testing.T) {
		result, err := componentSchema.GetOrGenerateSchema(context.Background(), fieldSchema)
		if err != nil || result.Type != integerOutput {
			t.Fatalf("unexpected result: %v %v", result, err)
		}
	})

	t.Run("create cached ref", func(t *testing.T) {
		container.generatedSchemas["Cached"] = &openapi3.Schema{Type: objectOutput}
		cached, err := container.createSchema(context.Background(), componentSchema, &Schema{path: "p", description: "d", rType: reflect.TypeOf(struct{}{}), tag: Tag{TypeName: "Cached"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cached.Ref != "#/components/schemas/Cached" {
			t.Fatalf("unexpected ref: %s", cached.Ref)
		}
	})

	t.Run("create struct and generate schema", func(t *testing.T) {
		type rec struct {
			ID int `json:"id"`
		}
		sch, err := container.createSchema(context.Background(), componentSchema, &Schema{
			path:        "rec",
			description: "record",
			rType:       reflect.TypeOf(rec{}),
			tag:         Tag{TypeName: "Rec"},
			ioConfig:    componentSchema.component.IOConfig(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sch.Ref == "" {
			t.Fatalf("expected ref schema")
		}
	})

	t.Run("addToSchema time format", func(t *testing.T) {
		dst := &openapi3.Schema{}
		err := container.addToSchema(context.Background(), componentSchema, dst, &Schema{
			rType: reflect.TypeOf(time.Time{}),
			tag:   Tag{_tag: format.Tag{TimeLayout: "2006-01-02"}},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst.Type != stringOutput || dst.Format != "date" {
			t.Fatalf("unexpected time schema: %s %s", dst.Type, dst.Format)
		}
	})

	t.Run("addToSchema struct filtering and inline", func(t *testing.T) {
		type payload struct {
			Visible  string            `json:"visible"`
			Hidden   string            `json:"-"`
			Internal string            `internal:"true"`
			Meta     map[string]string `json:",inline"`
		}
		dst := &openapi3.Schema{}
		err := container.addToSchema(context.Background(), componentSchema, dst, &Schema{
			rType:    reflect.TypeOf(payload{}),
			ioConfig: componentSchema.component.IOConfig(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := dst.Properties["visible"]; !ok {
			t.Fatalf("expected visible field")
		}
		if _, ok := dst.Properties["hidden"]; ok {
			t.Fatalf("did not expect hidden field")
		}
		if _, ok := dst.Properties["internal"]; ok {
			t.Fatalf("did not expect internal field")
		}
	})
}

func TestNewComponentSchema(t *testing.T) {
	component := &repository.Component{}
	got := NewComponentSchema(nil, component, nil)
	if got == nil || got.schemas == nil {
		t.Fatalf("expected initialized component schema")
	}
}
