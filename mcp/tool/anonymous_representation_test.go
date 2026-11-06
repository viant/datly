package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
)

type AnonymousMetadataEmbedded struct {
	Stable string `json:"stable"`
}

func (*AnonymousMetadataEmbedded) Linked() bool { return true }

type anonymousMetadataBody struct {
	*AnonymousMetadataEmbedded
	Mode   string                               `json:"mode" choice:"draft" choice:"approved"`
	Secret string                               `json:"secret"`
	Has    *struct{ Stable, Mode, Secret bool } `json:"-" setMarker:"true"`
}
type anonymousMetadataInput struct {
	Body *anonymousMetadataBody `anonymous:"true"`
}

func TestAnonymousBodyRetainsEffectiveFieldAnnotationsAndBinding(t *testing.T) {
	required := true
	canonical := testRouteContract(t, reflect.TypeFor[anonymousMetadataInput](), []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}, Required: &required}})
	walker, _ := shape.CompileRewrite(canonical.Type())
	metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[anonymousMetadataBody]() {
			switch f.Name {
			case "Secret":
				return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
			case "Mode":
				return &shape.FieldMetadata{Name: f.Name, Tag: `json:"workflow" mcp:"name=Workflow,aliases=legacy_workflow" choice:"draft" required:"true" description:"Draft mode" example:"draft"`}, nil
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fields, err := canonical.MetadataRepresentation(metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := NewCompiler().Compile(Input{Contract: canonical, Fields: fields, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "anonymous.metadata"}})
	if err != nil {
		t.Fatal(err)
	}
	schema := tool.Metadata().InputSchema
	if schema.Properties["secret"] != nil || schema.Properties["Has"] != nil || schema.Properties["has"] != nil {
		t.Fatal("private infrastructure or business field exposed")
	}
	field := schema.Properties["Workflow"]
	if field["description"] != "Draft mode" || !reflect.DeepEqual(field["enum"], []any{"draft"}) || !reflect.DeepEqual(field["examples"], []any{"draft"}) || !reflect.DeepEqual(schema.Required, []string{"Workflow"}) {
		t.Fatalf("flattened annotations lost: %+v required=%v", field, schema.Required)
	}
	execution, err := canonical.WithInputRepresentation(metadata)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := tool.Scope(map[string]any{"legacy_workflow": "draft", "secret": "blocked", "Has": map[string]any{"Secret": true}})
	if err != nil {
		t.Fatal(err)
	}
	defer providers.Close()
	injector, _ := bindly.NewInjector()
	scope, err := injector.ForScope(providers.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	var actual anonymousMetadataInput
	if err = scope.Bind(t.Context(), &actual, bindly.WithPlan(execution.Plan())); err != nil {
		t.Fatal(err)
	}
	if actual.Body == nil || actual.Body.Mode != "draft" || actual.Body.Secret != "" || actual.Body.Has == nil || !actual.Body.Has.Mode || actual.Body.Has.Secret || !actual.Body.Linked() {
		t.Fatal("canonical anonymous body binding/presence lost")
	}
}
