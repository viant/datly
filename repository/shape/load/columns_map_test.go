package load

import (
	"reflect"
	"testing"
)

func TestReflectDataType_MapStringInterface(t *testing.T) {
	got := reflectDataType(reflect.TypeOf(map[string]interface{}{}))
	if got != "map[string]interface{}" {
		t.Fatalf("expected map[string]interface{}, got %s", got)
	}
}

func TestReflectDataType_MapStringAny(t *testing.T) {
	got := reflectDataType(reflect.TypeOf(map[string]any{}))
	if got != "map[string]interface{}" {
		t.Fatalf("expected map[string]interface{}, got %s", got)
	}
}
