package openapi

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/x/shape"
)

type ContractEmbeddedBase struct {
	Label string `json:"label"`
}
type ContractEmbedded struct{ *ContractEmbeddedBase }

func (*ContractEmbedded) Linked() {}

type contractMember struct {
	Before int `json:"before"`
	*ContractEmbedded
	After  string                       `json:"after"`
	Secret string                       `json:"secret"`
	Mode   int                          `json:"mode" choice:"0:draft" choice:"10:active" enumAccept:"literal" enumStyle:"ordinal" required:"true"`
	Has    *struct{ Mode, Secret bool } `json:"-" setMarker:"true"`
}
type contractRoot struct {
	Rows map[string][][]*contractMember `json:"rows"`
}

func TestContractSchemaNestedEmbeddedEnum(t *testing.T) {
	original := reflect.TypeFor[contractRoot]()
	walker, err := shape.CompileRewrite(original)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[contractMember]() && f.Name == "Secret" {
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	container := NewContainer()
	for _, input := range []bool{true, false} {
		component := NewComponentSchema(nil, &repository.Component{View: &view.View{}}, container)
		root, err := component.GenerateSchema(context.Background(), &Schema{rType: original, representation: metadata, ioConfig: &config.IOConfig{}, isInput: input})
		if err != nil {
			t.Fatal(err)
		}
		if len(root.Required) != 0 {
			t.Fatal("plain field must be optional", root.Required)
		}
		var memberFound bool
		for _, item := range container.generatedSchemas {
			if item == nil || item.Properties["mode"] == nil {
				continue
			}
			if item.Properties["secret"] != nil || item.Properties["Has"] != nil || item.Properties["label"] == nil || item.Properties["before"] == nil || item.Properties["after"] == nil {
				t.Fatalf("embedded fields: %+v", item.Properties)
			}
			mode := item.Properties["mode"]
			want := "integer"
			if input {
				want = "string"
			}
			if mode.Type != want {
				continue
			}
			if len(mode.Enum) != 2 {
				t.Fatalf("mode schema: %+v", mode)
			}
			memberFound = true
			if !reflect.DeepEqual(item.Required, []string{"mode"}) {
				t.Fatalf("required: %v", item.Required)
			}
		}
		if !memberFound {
			t.Fatal("nested schema missing")
		}
	}
}
