package output

import (
	"context"
	"github.com/viant/x/shape"
	"reflect"
	"testing"
	"time"
)

func TestDeclaredTimeOutputNested(t *testing.T) {
	type row struct {
		Stamp *time.Time `json:"stamp,omitempty" format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02,timeEmpty=preserve,timezone=UTC,nullable=true"`
	}
	type body struct {
		Rows map[string][]row `json:"rows"`
	}
	zero := time.Time{}
	value := body{Rows: map[string][]row{"a": {{Stamp: &zero}}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[body]()})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := plan.Encode(context.Background(), "json", value)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded.Data) != `{"rows":{"a":[{"stamp":null}]}}` {
		t.Fatalf("got %s", encoded.Data)
	}
}

func TestDeclaredTimeOutputRepresentation(t *testing.T) {
	type row struct {
		Stamp *time.Time `json:"stamp,omitempty"`
	}
	type body struct {
		Rows []row `json:"rows"`
	}
	original := reflect.TypeFor[body]()
	walker, err := shape.CompileRewrite(original)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if field.Name == "Stamp" {
			return &shape.FieldMetadata{Name: field.Name, Tag: `json:"stamp,omitempty" format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02,timeEmpty=preserve,timezone=UTC,nullable=true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := (Compiler{}).Compile(CompileInput{Type: original})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := base.WithRepresentation(metadata)
	if err != nil {
		t.Fatal(err)
	}
	zero := time.Time{}
	value := body{Rows: []row{{Stamp: &zero}}}
	for _, tc := range []struct {
		plan *Plan
		want string
	}{{base, `{"rows":[{"stamp":"0001-01-01T00:00:00Z"}]}`}, {plan, `{"rows":[{"stamp":null}]}`}} {
		encoded, err := tc.plan.Encode(t.Context(), "json", value)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded.Data) != tc.want {
			t.Fatalf("got %s want %s", encoded.Data, tc.want)
		}
		if _, err := tc.plan.JSONSchema(); err != nil {
			t.Fatal(err)
		}
	}
}
