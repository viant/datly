package contract

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type codecVariantRecord struct {
	Secret string `json:"secret"`
	Mode   int    `json:"mode" choice:"0:draft" choice:"10:active"`
}

func (*codecVariantRecord) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct{ Role string }
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Role == "linked" {
		return nil, nil
	}
	role := state.Role
	return &xcontract.Policy{Name: "codec-role:" + role, Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[codecVariantRecord]() {
			return nil, nil
		}
		switch f.Name {
		case "Secret":
			if role == "reader" {
				return f.Clone().Hide(), nil
			}
		case "Mode":
			style := "ordinal"
			if role == "reader" {
				style = "literal"
			}
			return f.Clone().SetTag("enumAccept", style).SetTag("enumStyle", style), nil
		}
		return nil, nil
	}}, nil
}

type codecRoleBinder string

func (b codecRoleBinder) Bind(_ context.Context, target any, _ ...bindly.BindOption) error {
	reflect.ValueOf(target).Elem().FieldByName("Role").SetString(string(b))
	return nil
}

func TestContractCodecVariantsAlternateAndReuse(t *testing.T) {
	typ := reflect.TypeFor[codecVariantRecord]()
	plan, err := New(typ, 8)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := structjson.NewMarshaller(typ)
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]*shape.Representation{}
	for _, role := range []string{"reader", "admin", "reader", "linked", "admin", "reader", "linked"} {
		metadata, err := plan.ResolveRepresentation(t.Context(), codecRoleBinder(role))
		if err != nil {
			t.Fatal(err)
		}
		if role == "linked" {
			if metadata != nil {
				t.Fatal("linked contract lost")
			}
		} else {
			if prior := variants[role]; prior != nil && prior != metadata {
				t.Fatal("named variant was rebuilt")
			}
			variants[role] = metadata
		}
		expected := `{"secret":"shown","mode":10}`
		wire := expected
		secret := "shown"
		if role == "reader" {
			expected = `{"mode":"active"}`
			wire = `{"secret":"blocked","mode":"active"}`
			secret = ""
		}
		value := codecVariantRecord{Secret: "shown", Mode: 10}
		raw, err := retained.MarshalWithRepresentation(value, metadata)
		if err != nil || string(raw) != expected {
			t.Fatalf("retained %s %s %v", role, raw, err)
		}
		for _, standard := range []bool{false, true} {
			var raw []byte
			var err error
			var decoded codecVariantRecord
			if standard {
				raw, err = structjson.MarshalStandard(value, structjson.WithRepresentation(metadata))
			} else {
				raw, err = structjson.Marshal(value, structjson.WithRepresentation(metadata))
			}
			if err != nil || string(raw) != expected {
				t.Fatalf("role=%s standard=%t %s %v", role, standard, raw, err)
			}
			if standard {
				err = structjson.UnmarshalStandard([]byte(wire), &decoded, structjson.WithRepresentation(metadata))
			} else {
				err = structjson.Unmarshal([]byte(wire), &decoded, structjson.WithRepresentation(metadata))
			}
			if err != nil || decoded.Secret != secret || decoded.Mode != 10 {
				t.Fatalf("role=%s standard=%t %+v %v", role, standard, decoded, err)
			}
		}
	}
	if variants["reader"] == variants["admin"] {
		t.Fatal("role variants share metadata identity")
	}
}
