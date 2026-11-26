package contract

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
	"testing"
)

type cowRecord struct {
	Secret string `json:"secret"`
	Title  string `json:"title" required:"true"`
}

func (*cowRecord) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "cow", Field: func(_ string, _ reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		before := field.Tag
		var adjusted *xcontract.Field
		switch field.Name {
		case "Secret":
			adjusted = field.Hide()
		case "Title":
			adjusted = field.Optional()
		default:
			return nil, nil
		}
		if field.Tag != before || adjusted == field {
			return nil, fmt.Errorf("callback input was not read-only")
		}
		if adjusted.SetDescription("Selected") != adjusted {
			return nil, fmt.Errorf("chain did not reuse writable copy")
		}
		return adjusted, nil
	}}, nil
}
func TestFieldHelpersPreserveCallbackSnapshot(t *testing.T) {
	original := reflect.TypeFor[cowRecord]()
	plan, err := New(original, 8)
	if err != nil {
		t.Fatal(err)
	}
	binder, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	if metadata == nil || metadata.Field(0).Tag.Get("json") != "-" || metadata.Field(1).Tag.Get("required") != "" {
		t.Fatal("missing COW edits")
	}
	if original.Field(0).Tag.Get("json") != "secret" || original.Field(1).Tag.Get("required") != "true" {
		t.Fatal("linked type changed")
	}
}
