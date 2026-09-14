package collector

import (
	"reflect"

	"github.com/viant/xunsafe"
)

// appendRelationHolderValue is intentionally shared by both streaming visitor
// attachment and post-fetch reconciliation so holder append semantics stay in
// one place.
func appendRelationHolderValue(appender *xunsafe.Appender, holderType reflect.Type, value interface{}) {
	if appender == nil || holderType.Kind() != reflect.Slice {
		if appender != nil {
			appender.Append(value)
		}
		return
	}
	elemType := holderType.Elem()
	rv := reflect.ValueOf(value)
	if elemType.Kind() == reflect.Ptr {
		if rv.IsValid() && rv.Kind() == reflect.Ptr {
			appender.Append(value)
			return
		}
		ptr := reflect.New(elemType.Elem())
		ptr.Elem().Set(rv)
		appender.Append(ptr.Interface())
		return
	}
	if rv.IsValid() && rv.Kind() == reflect.Ptr && !rv.IsNil() {
		appender.Append(rv.Elem().Interface())
		return
	}
	appender.Append(value)
}
