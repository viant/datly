package output

import (
	"reflect"
	"strings"
	"time"

	"github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
)

// declaredTimeOutput selects the native encoder for explicitly declared time
// policies. Ordinary output retains its existing standard JSON behavior.
func declaredTimeOutput(t reflect.Type, metadata *xshape.Representation, active map[reflect.Type]bool) bool {
	if t == nil || active[t] {
		return false
	}
	active[t] = true
	defer delete(active, t)
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		var child *xshape.Representation
		if metadata != nil {
			child = metadata.Elem()
		}
		return declaredTimeOutput(t.Elem(), child, active)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			var child *xshape.Representation
			if metadata != nil {
				field = metadata.Field(i)
				child = metadata.FieldShape(i)
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if !field.IsExported() || name == "-" || field.Tag.Get("internal") == "true" || field.Tag.Get("setMarker") == "true" {
				continue
			}
			policy, err := format.Parse(field.Tag)
			if err == nil && policy != nil && policy.Ignore {
				continue
			}
			base := field.Type
			for base.Kind() == reflect.Pointer {
				base = base.Elem()
			}
			if base == reflect.TypeFor[time.Time]() && err == nil && policy.HasTimeInputPolicy() {
				return true
			}
			if declaredTimeOutput(field.Type, child, active) {
				return true
			}
		}
	}
	return false
}
