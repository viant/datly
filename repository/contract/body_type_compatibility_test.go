package contract

import (
	"github.com/viant/datly/view/state"
	"reflect"
	"testing"
)

type compatibilityMap map[string]any

func TestBodyTypeKeepsLegacyMapBoundary(t *testing.T) {
	record := reflect.TypeFor[struct{ ID int }]()
	for _, typ := range []reflect.Type{
		reflect.TypeFor[map[string]any](), reflect.TypeFor[map[string]string](), reflect.TypeFor[compatibilityMap](),
		reflect.TypeFor[*map[string]any](), reflect.TypeFor[[]map[string]any](), reflect.TypeFor[[]int](),
		reflect.TypeFor[int](), reflect.TypeFor[*int](), record, reflect.PointerTo(record), reflect.SliceOf(record),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			c := &Contract{}
			c.Input.Body.Schema = state.NewSchema(typ)
			expected := typ
			if typ.Kind() == reflect.Map {
				expected = reflect.TypeFor[struct{}]()
			}
			if got := c.BodyType(); got != expected {
				t.Fatalf("BodyType=%v want legacy %v", got, expected)
			}
			if c.Input.Body.Schema.Type() != typ {
				t.Fatal("declared request type was replaced")
			}
		})
	}
	if (&Contract{}).BodyType() != nil {
		t.Fatal("missing body schema changed")
	}
}
