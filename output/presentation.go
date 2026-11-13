package output

import (
	"encoding"
	"encoding/json"
	"fmt"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strconv"
	"strings"
)

// presentation compiles branches affected by visibility or scalar metadata. Codecs receive
// a typed serialization view; application output and database rows stay intact.
type presentation struct {
	source, target reflect.Type
	elem           *presentation
	fields         []presentationField
	// byIndex retains canonical field positions for request selection reuse.
	byIndex     []*presentation
	enumeration *choice.Enum
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
	excluded       exclusions
	selected       indexExcluder
	representation *xshape.Representation
	hideJSON       bool
	active         map[reflect.Type]bool
	compileEnums   bool
	authority      *presentation
}

func (e exclusions) presentation(typeOf reflect.Type, path string) (*presentation, error) {
	return (presentationCompiler{excluded: e}).compile(typeOf, path, nil)
}
func (c presentationCompiler) compile(typeOf reflect.Type, path string, indexes []int) (*presentation, error) {
	return c.compileShape(typeOf, path, indexes, c.representation, c.authority)
}
func (c presentationCompiler) compileShape(typeOf reflect.Type, path string, indexes []int, metadata *xshape.Representation, authority *presentation) (*presentation, error) {
	if authority != nil && authority.enumeration != nil {
		return authority, nil
	}
	if authority != nil && (c.selected == nil || !c.selected.AffectsIndexes(indexes)) {
		// The immutable authority already owns metadata and fixed exclusions.
		// Selection recompiles only branches affected by this request.
		return authority, nil
	}
	if (c.hideJSON || c.compileEnums) && typeOf != nil && typeOf.Kind() == reflect.Struct {
		if c.active == nil {
			c.active = map[reflect.Type]bool{}
		}
		if c.active[typeOf] {
			return nil, fmt.Errorf("recursive effective output requires metadata")
		}
		c.active[typeOf] = true
		defer delete(c.active, typeOf)
	}
	e := c.excluded
	if typeOf == nil || (c.selected == nil || !c.selected.AffectsIndexes(indexes)) && !e.affects(path) && !c.hideJSON && !metadata.Changed() && !c.compileEnums && authority == nil {
		return nil, nil
	}
	p := &presentation{source: typeOf}
	switch typeOf.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		var element *xshape.Representation
		if metadata != nil {
			element = metadata.Elem()
		}
		var childAuthority *presentation
		if authority != nil {
			childAuthority = authority.elem
		}
		child, err := c.compileShape(typeOf.Elem(), path, indexes, element, childAuthority)
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
		// Preserve opaque scalar/column owners and their method sets. Enum.Parse
		// independently rejects an active enum on a custom JSON/Text scalar.
		if presentationOpaque(typeOf) && !metadata.Changed() && !e.affects(path) && (c.selected == nil || !c.selected.AffectsIndexes(indexes)) {
			return nil, nil
		}
		fields, err := xshape.Linked(typeOf).Fields()
		if err != nil {
			return nil, err
		}
		var targetFields []xshape.RuntimeField
		p.byIndex = make([]*presentation, typeOf.NumField())
		changed := false
		for _, field := range fields {
			if !field.Exported || len(field.Index) != 1 {
				continue
			}
			effective := typeOf.Field(field.Index[0])
			var childMetadata *xshape.Representation
			if metadata != nil && metadata.NumField() > field.Index[0] {
				effective = metadata.Field(field.Index[0])
				childMetadata = metadata.FieldShape(field.Index[0])
			}
			alias, _, _ := strings.Cut(effective.Tag.Get("json"), ",")
			index := append(append([]int(nil), indexes...), field.Index...)
			if (metadata != nil || c.hideJSON) && alias == "-" || c.selected != nil && c.selected.ExcludeIndexes(index) || e.Exclude(path, field.Name) || alias != "" && e.Exclude(path, alias) {
				changed = true
				continue
			}
			childPath := field.Name
			if path != "" {
				childPath = path + "." + childPath
			}
			var childAuthority *presentation
			if authority != nil && len(authority.byIndex) > field.Index[0] {
				childAuthority = authority.byIndex[field.Index[0]]
			}
			var child *presentation
			if c.compileEnums {
				child, err = compileEnumPresentation(effective)
				if err != nil {
					return nil, fmt.Errorf("output field %s: %w", childPath, err)
				}
			}
			if child == nil {
				child, err = c.compileShape(field.ReflectedType, childPath, index, childMetadata, childAuthority)
			}
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
			if effective.Name != field.Name || effective.Tag != field.Tag {
				changed = true
			}
			p.fields = append(p.fields, presentationField{source: field.Index, target: len(targetFields), projection: child})
			p.byIndex[field.Index[0]] = child
			targetFields = append(targetFields, xshape.RuntimeField{Name: effective.Name, Type: targetType, Tag: effective.Tag, Anonymous: anonymous})
		}
		if !changed {
			return nil, nil
		}
		p.target, err = (xshape.Runtime{}).Struct(targetFields)
		if err != nil {
			return nil, err
		}
	default:
		if metadata.Changed() || c.hideJSON && (typeOf.Kind() == reflect.Map || typeOf.Kind() == reflect.Interface) {
			return nil, fmt.Errorf("unsupported effective export shape %v", typeOf)
		}
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
	if p.enumeration != nil {
		wire, err := p.enumeration.Encode(source.Interface())
		if err != nil {
			return reflect.Value{}, err
		}
		if wire == nil {
			return reflect.Zero(p.target), nil
		}
		target := reflect.New(p.target).Elem()
		value := target
		for value.Kind() == reflect.Pointer {
			value.Set(reflect.New(value.Type().Elem()))
			value = value.Elem()
		}
		if number, ok := wire.(json.Number); ok {
			if value.Kind() >= reflect.Int && value.Kind() <= reflect.Int64 {
				integer, err := strconv.ParseInt(string(number), 10, value.Type().Bits())
				if err != nil {
					return reflect.Value{}, err
				}
				value.SetInt(integer)
			} else {
				integer, err := strconv.ParseUint(string(number), 10, value.Type().Bits())
				if err != nil {
					return reflect.Value{}, err
				}
				value.SetUint(integer)
			}
		} else {
			value.Set(reflect.ValueOf(wire).Convert(value.Type()))
		}
		return target, nil
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

func presentationOpaque(typ reflect.Type) bool {
	for _, owner := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if typ.Implements(owner) || reflect.PointerTo(typ).Implements(owner) {
			return true
		}
	}
	return false
}

// compileEnumPresentation delegates declaration parsing, exact alias matching,
// and unknown-value policy to Tagly. Only the typed wire carrier belongs here.
func compileEnumPresentation(field reflect.StructField) (*presentation, error) {
	base := field.Type
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	switch base.Kind() {
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	default:
		// Collection and union codecs retain their existing owners.
		return nil, nil
	}
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil || enumeration == nil {
		return nil, err
	}
	var target reflect.Type
	switch enumeration.WireType(false) {
	case "string":
		target = reflect.TypeFor[string]()
	case "integer":
		// Keep signedness and full integer precision for native numeric cells.
		target = enumeration.Type
		if target.Kind() == reflect.String {
			target = reflect.TypeFor[int64]()
			unsigned, negative := false, false
			for _, entry := range enumeration.Entries {
				if strings.HasPrefix(string(entry.Ordinal), "-") {
					negative = true
				}
				if _, err := strconv.ParseInt(string(entry.Ordinal), 10, 64); err != nil {
					unsigned = true
				}
			}
			if unsigned && negative {
				return nil, fmt.Errorf("enum ordinals exceed a single integer wire type")
			}
			if unsigned {
				target = reflect.TypeFor[uint64]()
			}
		}
	default:
		return nil, fmt.Errorf("enum output requires a single scalar wire type")
	}
	for source := field.Type; source.Kind() == reflect.Pointer; source = source.Elem() {
		target = reflect.PointerTo(target)
	}
	return &presentation{source: field.Type, target: target, enumeration: enumeration}, nil
}
