package tool

import (
	"reflect"
	"testing"
)

type hiddenParent struct {
	Secret string `json:"secret"`
}
type publicParent struct {
	Shadow string `json:"shadow"`
}
type internalToolInput struct {
	*hiddenParent `internal:"true"`
	publicParent
	Shadow      string `json:"shadow" internal:"true"`
	Public      string `json:"public" format:"-"`
	Private     string `json:"private" internal:"true" mcp:"name=exposed,aliases=other"`
	Unavailable string `internal:"true" mcp:"unsupported=secret"`
}

func TestInternalPublicInputSchemaAuthority(t *testing.T) {
	schema, err := schemaForType(reflect.TypeFor[internalToolInput](), nil)
	if err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]interface{})
	if len(properties) != 1 || properties["public"] == nil {
		t.Fatalf("internal input leaked: %v", properties)
	}
	f, _ := reflect.TypeFor[internalToolInput]().FieldByName("Private")
	_, _, hidden, err := publicFieldName("Private", "private", f)
	if err != nil || !hidden {
		t.Fatalf("MCP alias bypassed internal: hidden=%v error=%v", hidden, err)
	}
	f, _ = reflect.TypeFor[internalToolInput]().FieldByName("Unavailable")
	_, _, hidden, err = publicFieldName("Unavailable", "", f)
	if err != nil || !hidden {
		t.Fatalf("internal metadata was inspected: hidden=%v error=%v", hidden, err)
	}
}
