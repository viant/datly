// Package inputvisibility applies public-input policy to already selected native
// JSON fields. It never discovers fields or inspects invocation values.
package inputvisibility

import "reflect"

// Internal checks the winning field and its embedding owners after native JSON
// dominance has been resolved. Excluding a private winner must not reveal a
// shadowed public field. Callers use this only while compiling public metadata.
func Internal(owner reflect.Type, index []int) bool {
	for _, position := range index {
		for owner.Kind() == reflect.Pointer {
			owner = owner.Elem()
		}
		field := owner.Field(position)
		if field.Tag.Get("internal") == "true" {
			return true
		}
		owner = field.Type
	}
	return false
}
