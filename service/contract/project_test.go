package contract

import (
	"github.com/viant/x/shape"
	"reflect"
	"testing"
)

func TestProjectPreservesTransportTags(t *testing.T) {
	type linked struct {
		Value int `parameter:",kind=body,in=value" choice:"0:draft" choice:"10:active"`
	}
	type transport struct {
		Value int `json:"value" required:"true" description:"transport" choice:"0:draft" choice:"10:active"`
	}
	walker, _ := shape.CompileRewrite(reflect.TypeFor[linked]())
	source, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		return &shape.FieldMetadata{Name: f.Name, Tag: `parameter:",kind=body,in=value" choice:"10:active" enumAccept:"literal"`}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := Project(reflect.TypeFor[transport](), source, func(path string) string { return path })
	if err != nil {
		t.Fatal(err)
	}
	tag := projected.Field(0).Tag
	if tag.Get("json") != "value" || tag.Get("description") != "transport" || tag.Get("enumAccept") != "literal" || tag.Get("choice") != "10:active" {
		t.Fatalf("%s", tag)
	}
	if projected.Original() != reflect.TypeFor[transport]() {
		t.Fatal("changed storage")
	}
}

func TestProjectOptionalRemovesTransportRequiredFlag(t *testing.T) {
	original := reflect.StructTag(`parameter:",kind=body,in=value,required=true"`)
	selected := reflect.StructTag(`parameter:",kind=body,in=value"`)
	projected := reflect.StructTag(`json:"value" required:"true" parameter:",kind=body,in=value,required=true"`)
	tag, err := ProjectTags(projected, original, selected)
	if err != nil {
		t.Fatal(err)
	}
	if tag.Get("required") != "" || tag.Get("json") != "value" {
		t.Fatalf("%s", tag)
	}
}
