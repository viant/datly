package dql

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"reflect"
)

// applySelectorTag imports the existing tag contract into canonical authoring
// metadata before generation removes reserved tags and emits their canonical form.
func applySelectorTag(param *spec.Parameter) error {
	raw := reflect.StructTag(tag.CanonicalFieldTag(param.Tag)).Get(tag.QuerySelectorName)
	if raw == "" {
		return nil
	}
	selector, err := tag.ParseQuerySelector(raw)
	if err != nil {
		return err
	}
	if selector.Property == "" {
		property, ok := spec.SelectorPropertyForParam(param.Name)
		if !ok {
			return fmt.Errorf("query selector %s requires an explicit property", param.Name)
		}
		selector.Property = property
	}
	binding := &spec.QuerySelectorBinding{View: selector.View, Property: selector.Property}
	if param.QuerySelector != nil && *param.QuerySelector != *binding {
		return fmt.Errorf("query selector tag conflicts with declared selector")
	}
	param.QuerySelector = binding
	if param.Cacheable == nil {
		value := false
		param.Cacheable = &value
	}
	return nil
}
