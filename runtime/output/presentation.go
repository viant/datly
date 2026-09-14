package output

import (
	"fmt"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// presentation compiles only branches affected by an exclusion. Codecs receive
// a typed serialization view; application output and database rows stay intact.
type presentation struct {
	source, target reflect.Type
	elem           *presentation
	fields         []presentationField
}
type presentationField struct {
	source     []int
	target     int
	projection *presentation
}

type indexExcluder interface {
	ExcludeIndexes([]int) bool
	AffectsIndexes([]int) bool
}
type presentationCompiler struct {
	excluded exclusions
	selected indexExcluder
}

func (e exclusions) presentation(typeOf reflect.Type, path string) (*presentation, error) {
	return (presentationCompiler{excluded: e}).compile(typeOf, path, nil)
}
func (c presentationCompiler) compile(typeOf reflect.Type, path string, indexes []int) (*presentation, error) {
	e := c.excluded
	if typeOf == nil || (c.selected == nil || !c.selected.AffectsIndexes(indexes)) && !e.affects(path) {
		return nil, nil
	}
	p := &presentation{source: typeOf}
	switch typeOf.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		child, err := c.compile(typeOf.Elem(), path, indexes)
		if err != nil || child == nil {
			return nil, err
		}
		p.elem = child
		switch typeOf.Kind() {
		case reflect.Pointer:
			p.target = (xshape.Runtime{}).Pointer(child.target)
		case reflect.Slice:
			p.target = reflect.SliceOf(child.target)
		case reflect.Array:
			p.target = reflect.ArrayOf(typeOf.Len(), child.target)
		}
	case reflect.Struct:
		fields, err := xshape.Linked(typeOf).Fields()
		if err != nil {
			return nil, err
		}
		var targetFields []xshape.RuntimeField
		changed := false
		for _, field := range fields {
			if !field.Exported || len(field.Index) != 1 {
				continue
			}
			alias, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			index := append(append([]int(nil), indexes...), field.Index...)
			if c.selected != nil && c.selected.ExcludeIndexes(index) || e.Exclude(path, field.Name) || alias != "" && e.Exclude(path, alias) {
				changed = true
				continue
			}
			childPath := field.Name
			if path != "" {
				childPath = path + "." + childPath
			}
			child, err := c.compile(field.ReflectedType, childPath, index)
			if err != nil {
				return nil, err
			}
			targetType := field.ReflectedType
			anonymous := field.Anonymous
			if child != nil {
				targetType = child.target
				changed = true
				if targetType.Name() == "" {
					anonymous = false
				}
			}
			p.fields = append(p.fields, presentationField{source: field.Index, target: len(targetFields), projection: child})
			targetFields = append(targetFields, xshape.RuntimeField{Name: field.Name, Type: targetType, Tag: field.Tag, Anonymous: anonymous})
		}
		if !changed {
			return nil, nil
		}
		p.target, err = (xshape.Runtime{}).Struct(targetFields)
		if err != nil {
			return nil, err
		}
	default:
		return nil, nil
	}
	return p, nil
}
func (e exclusions) affects(path string) bool {
	if path == "" {
		return len(e) > 0
	}
	prefix := strings.ToLower(path) + "."
	for _, item := range e {
		if strings.HasPrefix(strings.ToLower(item), prefix) {
			return true
		}
	}
	return false
}
func (p *presentation) value(source reflect.Value) (reflect.Value, error) {
	if p == nil {
		return source, nil
	}
	if !source.IsValid() || source.Type() != p.source {
		return reflect.Value{}, fmt.Errorf("output projection requires %v", p.source)
	}
	if (source.Kind() == reflect.Pointer || source.Kind() == reflect.Slice) && source.IsNil() {
		return reflect.Zero(p.target), nil
	}
	switch source.Kind() {
	case reflect.Pointer:
		value, err := p.elem.value(source.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		target := reflect.New(p.target.Elem())
		target.Elem().Set(value)
		return target, nil
	case reflect.Slice, reflect.Array:
		var target reflect.Value
		if source.Kind() == reflect.Slice {
			target = reflect.MakeSlice(p.target, source.Len(), source.Len())
		} else {
			target = reflect.New(p.target).Elem()
		}
		for i := 0; i < source.Len(); i++ {
			value, err := p.elem.value(source.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Index(i).Set(value)
		}
		return target, nil
	case reflect.Struct:
		target := reflect.New(p.target).Elem()
		for _, field := range p.fields {
			value, err := field.projection.value(source.FieldByIndex(field.source))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Field(field.target).Set(value)
		}
		return target, nil
	}
	return source, nil
}
