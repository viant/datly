package compiler

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
)

// Input-only pseudo fields have no database value to read into Previous.
// Retain a transient derived key only when canonical identity/relation metadata
// actually requires it; currentReadTag then maps that read-only key to its SQL alias.
func (b *inputGeneration) omitTransientCurrentColumn(view *spec.View, column *spec.Column) bool {
	tag := reflect.StructTag(column.Tag).Get("sqlx")
	if strings.TrimSpace(strings.Split(tag, ",")[0]) != "-" || column.PrimaryKey || effectivePrimaryKey(column) {
		return false
	}
	matches := func(name string) bool {
		return name != "" && (strings.EqualFold(name, column.Name) || strings.EqualFold(name, column.Source))
	}
	if self := view.SelfReference; self != nil && (matches(self.Parent) || matches(self.Child)) {
		return false
	}
	target, _ := view.Identity()
	same := func(v *spec.View) bool {
		if v == nil {
			return false
		}
		id, e := v.Identity()
		return e == nil && id == target
	}
	seen := map[*spec.View]bool{}
	var linked func(*spec.View) bool
	linked = func(parent *spec.View) bool {
		if parent == nil || seen[parent] {
			return false
		}
		seen[parent] = true
		for _, relation := range parent.Relations {
			if relation == nil {
				continue
			}
			for _, on := range relation.On {
				if on != nil && (same(parent) && matches(on.ParentColumn) || same(relation.View) && matches(on.ChildColumn)) {
					return true
				}
			}
			if linked(relation.View) {
				return true
			}
		}
		return false
	}
	if linked(b.request.Component.RootView) {
		return false
	}
	for _, other := range b.request.Component.Views {
		if linked(other) {
			return false
		}
	}
	return true
}
