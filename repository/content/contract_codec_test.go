package content

import (
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/x/shape"
	"github.com/viant/xreflect"
	"reflect"
	"testing"
)

type contractCodec struct{}

func (*contractCodec) Marshal(value any) ([]byte, error) { return structjson.Marshal(value) }
func (*contractCodec) Unmarshal(data []byte, value any) error {
	return structjson.Unmarshal(data, value)
}
func (*contractCodec) MarshalWithRepresentation(value any, metadata *shape.Representation) ([]byte, error) {
	return structjson.Marshal(value, structjson.WithRepresentation(metadata))
}
func (*contractCodec) UnmarshalWithRepresentation(data []byte, value any, metadata *shape.Representation) error {
	return structjson.Unmarshal(data, value, structjson.WithRepresentation(metadata))
}
func TestContractConfiguredCodecCapability(t *testing.T) {
	codec := &JSON{MarshalTypeName: "custom", UnmarshalTypeName: "custom"}
	if err := codec.Init(func(_ string, _ ...xreflect.Option) (reflect.Type, error) {
		return reflect.TypeFor[contractCodec](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if codec.ContractEncoder() == nil || codec.ContractDecoder() == nil {
		t.Fatal("lost configured capability")
	}
	type value struct {
		Secret string `json:"secret"`
		Value  int    `json:"value"`
	}
	walker, _ := shape.CompileRewrite(reflect.TypeFor[value]())
	metadata, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if f.Name == "Secret" {
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var result value
	if err = codec.ContractDecoder().UnmarshalWithRepresentation([]byte(`{"secret":"blocked","value":3}`), &result, metadata); err != nil {
		t.Fatal(err)
	}
	if result.Secret != "" || result.Value != 3 {
		t.Fatalf("%+v", result)
	}
	data, err := codec.ContractEncoder().MarshalWithRepresentation(value{Secret: "blocked", Value: 3}, metadata)
	if err != nil || string(data) != `{"value":3}` {
		t.Fatalf("%s %v", data, err)
	}
}
