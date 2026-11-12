package repository

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type ContractEmbedded struct {
	Label string `json:"label"`
}

func (*ContractEmbedded) Linked() {}

type contractRow struct {
	*ContractEmbedded
	Secret string                       `json:"secret"`
	Mode   int                          `json:"mode" choice:"0:draft" choice:"10:active"`
	Has    *struct{ Secret, Mode bool } `json:"-" setMarker:"true"`
}

func (*contractRow) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct{ Role string }
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Role == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "reader", Field: func(path string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[contractRow]() {
			return nil, nil
		}
		switch f.Name {
		case "Secret":
			return f.Hide(), nil
		case "Mode":
			return f.SetTag("enumAccept", "literal").SetTag("enumStyle", "ordinal"), nil
		}
		return nil, nil
	}}, nil
}

type contractEnvelope struct {
	Rows map[string][][]*contractRow `json:"rows"`
}
type testContractBinder string

func (b testContractBinder) Bind(_ context.Context, target any, _ ...bindly.BindOption) error {
	reflect.ValueOf(target).Elem().FieldByName("Role").SetString(string(b))
	return nil
}

func TestContractConfiguredJSONNestedEmbeddings(t *testing.T) {
	for _, standard := range []bool{false, true} {
		name := "native"
		if standard {
			name = "standard"
		}
		t.Run(name, func(t *testing.T) { testContractConfiguredJSON(t, standard) })
	}
}
func testContractConfiguredJSON(t *testing.T, standard bool) {
	c := &Component{}
	c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[contractEnvelope]())
	c.Content.Marshaller.JSON.RuntimeMarshaller = &content.StructologyJSONRuntime{}
	c.Content.Marshaller.JSON.RuntimeUnmarshaller = &content.StructologyJSONRuntime{}
	if standard {
		c.Content.Marshaller.JSON.RuntimeMarshaller = &content.StandardJSONRuntime{}
		c.Content.Marshaller.JSON.RuntimeUnmarshaller = &content.StandardJSONRuntime{}
	}
	selected, err := c.ResolveContract(context.Background(), testContractBinder("reader"), "PATCH", "/campaigns")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Output == nil {
		t.Fatal("missing metadata")
	}
	original := contractEnvelope{Rows: map[string][][]*contractRow{"a": {{{ContractEmbedded: &ContractEmbedded{Label: "linked"}, Secret: "private", Mode: 10}}}}}
	data, err := c.MarshalFunc(WithRepresentation(selected.Output))(original)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	item := doc["rows"].(map[string]any)["a"].([]any)[0].([]any)[0].(map[string]any)
	if item["secret"] != nil || item["label"] != "linked" || item["mode"] != float64(10) {
		t.Fatalf("%s", data)
	}
	var decoded contractEnvelope
	err = c.UnmarshalFor(WithUnmarshalRepresentation(selected.Output))([]byte(`{"rows":{"a":[[{"label":"still linked","secret":"blocked","mode":"active"}]]}}`), &decoded)
	if err != nil {
		t.Fatal(err)
	}
	row := decoded.Rows["a"][0][0]
	if row.Secret != "" || row.Mode != 10 || row.Label != "still linked" || row.Has == nil || !row.Has.Mode || row.Has.Secret {
		t.Fatalf("%+v", row)
	}
	again, err := c.ResolveContract(context.Background(), testContractBinder("reader"), "PATCH", "/campaigns")
	if err != nil || again.Output != selected.Output {
		t.Fatal("named metadata cache missed", err)
	}
	admin, err := c.ResolveContract(context.Background(), testContractBinder("admin"), "PATCH", "/campaigns")
	if err != nil || admin.Output != nil {
		t.Fatal("unchanged linked type lost", err)
	}
	if reflect.TypeOf(original.Rows["a"][0][0]) != reflect.TypeFor[*contractRow]() {
		t.Fatal("storage changed")
	}
}

type namedContractInput struct {
	First  *contractRow `parameter:",kind=body,in=first,required=true"`
	Second *contractRow `parameter:",kind=body,in=second"`
}

func (*namedContractInput) Contract(_ context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "parent", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[namedContractInput]() {
			return nil, nil
		}
		switch f.Name {
		case "First":
			return f.Optional(), nil
		case "Second":
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func TestContractNamedBodyProjection(t *testing.T) {
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[namedContractInput]())))
	if err != nil {
		t.Fatal(err)
	}
	bodyType, err := input.Parameters.BuildBodyType("contracttest", extension.Config.Types.Lookup)
	if err != nil {
		t.Fatal(err)
	}
	c := &Component{}
	c.Input.Type = *input
	c.Input.Body.Schema = state.NewSchema(bodyType)
	c.Content.Marshaller.JSON.RuntimeUnmarshaller = &content.StructologyJSONRuntime{}
	effective, err := c.ResolveContract(context.Background(), testContractBinder("reader"), "PATCH", "/named")
	if err != nil {
		t.Fatal(err)
	}
	original := bodyType
	for original.Kind() == reflect.Pointer {
		original = original.Elem()
	}
	target := reflect.New(original)
	if err = c.UnmarshalFor(WithUnmarshalRepresentation(effective.Body))([]byte(`{"first":{"secret":"blocked","mode":"active"},"second":{"mode":"active"}}`), target.Interface()); err != nil {
		t.Fatal(err)
	}
	first := target.Elem().FieldByName("First").Interface().(*contractRow)
	if first.Secret != "" || first.Mode != 10 || first.Has == nil || !first.Has.Mode || first.Has.Secret {
		t.Fatalf("first: %+v", first)
	}
	if !target.Elem().FieldByName("Second").IsNil() {
		t.Fatal("parent-hidden branch populated")
	}
	bodyMetadata := effective.Body
	for bodyMetadata.Original().Kind() == reflect.Pointer {
		bodyMetadata = bodyMetadata.Elem()
	}
	firstField, _ := bodyMetadata.Original().FieldByName("First")
	if bodyMetadata.Field(firstField.Index[0]).Tag.Get("required") == "true" {
		t.Fatal("transport schema retained synthetic required flag")
	}
	selected, hidden := EffectiveParameter(input.Parameters.Lookup("First"), effective.Input)
	if hidden || selected.IsRequired() {
		t.Fatal("optional native parameter remained required")
	}
}

type generationPolicyContextKey struct{}
type generationPolicyRow struct{ Value int }

func (*generationPolicyRow) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	description := ctx.Value(generationPolicyContextKey{}).(string)
	return &xcontract.Policy{Name: "role", Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		return f.SetDescription(description), nil
	}}, nil
}
func TestContractGenerationResetsNamedVariants(t *testing.T) {
	c := &Component{}
	c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[generationPolicyRow]())
	first, err := c.ResolveContract(context.WithValue(context.Background(), generationPolicyContextKey{}, "first"), testContractBinder("reader"), "GET", "/row")
	if err != nil {
		t.Fatal(err)
	}
	c.Version.Increase()
	second, err := c.ResolveContract(context.WithValue(context.Background(), generationPolicyContextKey{}, "second"), testContractBinder("reader"), "GET", "/row")
	if err != nil {
		t.Fatal(err)
	}
	if first.Output.Field(0).Tag.Get("description") != "first" || second.Output.Field(0).Tag.Get("description") != "second" {
		t.Fatal("generation cache retained stale metadata")
	}
}

func TestContractDescriptionDoesNotClearExternalRequiredness(t *testing.T) {
	type input struct {
		Name string `parameter:",kind=query,in=name"`
	}
	walker, _ := shape.CompileRewrite(reflect.TypeFor[input]())
	metadata, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		return &shape.FieldMetadata{Name: f.Name, Tag: `parameter:",kind=query,in=name" description:"caller-specific"`}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	required := true
	parameter := state.NewParameter("Name", state.NewQueryLocation("name"), state.WithParameterSchema(state.NewSchema(reflect.TypeFor[string]())))
	parameter.Required = &required
	selected, hidden := EffectiveParameter(parameter, metadata)
	if hidden || !selected.IsRequired() {
		t.Fatal("unrelated tag change cleared native requiredness")
	}
}
