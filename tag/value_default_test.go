package tag

import (
	"reflect"
	"testing"
)

func TestLinkedSeparateValueDefault(t *testing.T) {
	for _, tc := range []struct {
		name, tag string
		want      any
	}{
		{"separate", `parameter:",kind=const,in=root" value:"/default"`, "/default"},
		{"empty", `parameter:",kind=const,in=root" value:""`, ""},
		{"inline precedence", `parameter:",kind=const,in=root,value=inline" value:"separate"`, "inline"},
		{"inline empty precedence", `parameter:",kind=const,in=root,value=''" value:"separate"`, ""},
		{"absent", `parameter:",kind=const,in=root"`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field, err := ParseField(reflect.StructField{Name: "Root", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(tc.tag)})
			if err != nil {
				t.Fatal(err)
			}
			if field.Binding.DefaultValue != tc.want {
				t.Fatalf("got %#v want %#v", field.Binding.DefaultValue, tc.want)
			}
		})
	}
}
