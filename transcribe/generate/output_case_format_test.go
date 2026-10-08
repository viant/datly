package generate

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestResolveGeneratedOutputUsesAuthoredCaseFormat(t *testing.T) {
	component, err := parseTestComponentSource("example.com/demo/items", "Items", `#setting($_ = $route('/items','GET'))
#setting($_ = $case_format('lc'))
#define($_ = $Violations<[]string>(output/violations))
#define($_ = $RequestID<string>(output/requestId).WithTag('json:"request_id"'))
SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(t, component)
	var fields []reflect.StructField
	for _, field := range plan.Output.Fields {
		if field.Name == "Violations" {
			fields = append(fields, reflect.StructField{Name: field.Name, Type: reflect.TypeFor[[]string](), Tag: reflect.StructTag(field.Tag)})
		}
		if field.Name == "RequestID" && reflect.StructTag(field.Tag).Get("json") != "request_id" {
			t.Fatal("explicit naming was changed")
		}
	}
	if len(fields) != 1 {
		t.Fatal("missing output field")
	}
	value := reflect.New(reflect.StructOf(fields)).Elem()
	value.Field(0).Set(reflect.ValueOf([]string{"invalid"}))
	raw, err := json.Marshal(value.Interface())
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"violations":["invalid"]}` {
		t.Fatalf("typed error envelope naming: %s", raw)
	}
}
