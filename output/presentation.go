package output

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
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
	jsonCell    func(context.Context, any) ([]byte, error)
	path        string
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
	active         map[reflect.Type][]int
	cell           func(reflect.Type, *xshape.Representation) (func(context.Context, any) ([]byte, error), error)
	compileEnums   bool
	authority      *presentation
	bodyScope      []int
	envelope       bool
	canonical      reflect.Type
	fieldVisible   func(reflect.StructField, reflect.StructField, string) bool
}

func (e exclusions) presentation(typeOf reflect.Type, path string) (*presentation, error) {
	return (presentationCompiler{excluded: e}).compile(typeOf, path, nil)
}
func (c presentationCompiler) compile(typeOf reflect.Type, path string, indexes []int) (*presentation, error) {
	return c.compileShape(typeOf, path, indexes, c.representation, c.authority)
}

// recursivePresentation carries the first occurrence's canonical field, so the
// owning field retains its complete pointer/slice container as one JSON cell.
type recursivePresentation struct{ indexes []int }

func (*recursivePresentation) Error() string {
	return "recursive effective output requires a complete unchanged container"
}

func (c presentationCompiler) compileShape(typeOf reflect.Type, path string, indexes []int, metadata *xshape.Representation, authority *presentation) (*presentation, error) {
	if authority != nil && authority.jsonCell != nil {
		if c.selected != nil && c.selected.AffectsIndexes(indexes) {
			kind := "recursive"
			if opaqueCellContainer(authority.source) {
				kind = "opaque"
			}
			return nil, fmt.Errorf("partial %s output selection at %s is unsupported", kind, path)
		}
		return authority, nil
	}
	if authority != nil && authority.enumeration != nil {
		return authority, nil
	}
	if authority != nil && (c.selected == nil || !c.selected.AffectsIndexes(indexes)) {
		// The immutable authority already owns metadata and fixed exclusions.
		// Selection recompiles only branches affected by this request.
		return authority, nil
	}
	e := c.excluded
	// Opaque JSON/Text owners keep their complete value and method set. This
	// also covers named recursive containers with an explicit scalar owner.
	if typeOf != nil && presentationOpaque(typeOf) && !metadata.Changed() && !e.affects(path) && (c.selected == nil || !c.selected.AffectsIndexes(indexes)) {
		return nil, nil
	}
	if (c.hideJSON || c.compileEnums) && typeOf != nil && (typeOf.Kind() == reflect.Struct || typeOf.Kind() == reflect.Pointer || typeOf.Kind() == reflect.Slice || typeOf.Kind() == reflect.Array) {
		if c.active == nil {
			c.active = map[reflect.Type][]int{}
		}
		if first, ok := c.active[typeOf]; ok {
			return nil, &recursivePresentation{indexes: first}
		}
		c.active[typeOf] = append([]int(nil), indexes...)
		defer delete(c.active, typeOf)
	}
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
			if len(c.bodyScope) > 0 && len(indexes) == 0 && field.Index[0] != c.bodyScope[0] {
				changed = true
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
			if (metadata != nil || c.hideJSON || c.cell != nil || c.authority != nil) && alias == "-" || c.fieldVisible != nil && !c.fieldVisible(typeOf.Field(field.Index[0]), effective, path) || c.selected != nil && c.selected.ExcludeIndexes(index) || e.Exclude(path, field.Name) || alias != "" && e.Exclude(path, alias) {
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
			if child == nil && c.cell != nil && opaqueCellContainer(field.ReflectedType) {
				if childMetadata.Changed() || e.affects(childPath) || c.selected != nil && c.selected.AffectsIndexes(index) || c.canonical != nil && canonicalFieldType(c.canonical, index) != field.ReflectedType {
					return nil, fmt.Errorf("opaque output field %s requires complete unchanged selection", childPath)
				}
				encode, cellErr := c.cell(field.ReflectedType, childMetadata)
				if cellErr != nil {
					return nil, fmt.Errorf("prepare opaque output field %s: %w", childPath, cellErr)
				}
				child = &presentation{source: field.ReflectedType, target: reflect.TypeFor[string](), jsonCell: encode, path: childPath}
				if c.envelope && len(indexes) == 0 {
					carrier, carrierErr := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: effective.Name, Type: reflect.TypeFor[string](), Tag: effective.Tag}})
					if carrierErr != nil {
						return nil, carrierErr
					}
					child.target = carrier
				}
			}
			if child == nil {
				child, err = c.compileShape(field.ReflectedType, childPath, index, childMetadata, childAuthority)
			}
			if err != nil {
				var recursive *recursivePresentation
				if !errors.As(err, &recursive) || !reflect.DeepEqual(recursive.indexes, index) || c.cell == nil {
					return nil, err
				}
				if childMetadata.Changed() || e.affects(childPath) || c.selected != nil && c.selected.AffectsIndexes(index) {
					return nil, fmt.Errorf("recursive output field %s requires complete unchanged selection: %w", childPath, err)
				}
				encode, cellErr := c.cell(field.ReflectedType, childMetadata)
				if cellErr != nil {
					return nil, fmt.Errorf("prepare recursive output field %s: %w", childPath, cellErr)
				}
				child = &presentation{source: field.ReflectedType, target: reflect.TypeFor[string](), jsonCell: encode, path: childPath}
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
	return p.valueContext(context.Background(), source)
}
func (p *presentation) valueContext(ctx context.Context, source reflect.Value) (reflect.Value, error) {
	return p.valueContextScope(ctx, source, nil)
}
func (p *presentation) valueContextScope(ctx context.Context, source reflect.Value, scope []int) (reflect.Value, error) {
	if p == nil {
		return source, nil
	}
	if !source.IsValid() || source.Type() != p.source {
		return reflect.Value{}, fmt.Errorf("output projection requires %v", p.source)
	}
	if p.jsonCell != nil {
		data, err := p.jsonCell(ctx, source.Interface())
		if err != nil {
			return reflect.Value{}, fmt.Errorf("encode JSON output field %s: %w", p.path, err)
		}
		value := reflect.ValueOf(string(data))
		if p.target.Kind() == reflect.Struct {
			carrier := reflect.New(p.target).Elem()
			carrier.Field(0).Set(value)
			return carrier, nil
		}
		return value, nil
	}
	if p.enumeration != nil {
		wire, err := p.enumeration.Encode(source.Interface())
		if err != nil {
			return reflect.Value{}, err
		}
		return enumPresentationValue(p.target, wire)
	}
	if (source.Kind() == reflect.Pointer || source.Kind() == reflect.Slice) && source.IsNil() {
		return reflect.Zero(p.target), nil
	}
	switch source.Kind() {
	case reflect.Pointer:
		value, err := p.elem.valueContextScope(ctx, source.Elem(), scope)
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
			value, err := p.elem.valueContext(ctx, source.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Index(i).Set(value)
		}
		return target, nil
	case reflect.Struct:
		target := reflect.New(p.target).Elem()
		for _, field := range p.fields {
			if len(scope) > 0 && field.source[0] != scope[0] {
				continue
			}
			value, err := field.projection.valueContext(ctx, source.FieldByIndex(field.source))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Field(field.target).Set(value)
		}
		return target, nil
	}
	return source, nil
}

func enumPresentationValue(targetType reflect.Type, wire any) (reflect.Value, error) {
	if wire == nil {
		return reflect.Zero(targetType), nil
	}
	target := reflect.New(targetType).Elem()
	value := target
	for value.Kind() == reflect.Pointer {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}
	if value.Kind() == reflect.Slice {
		items := reflect.ValueOf(wire)
		if items.Kind() == reflect.Slice && items.IsNil() {
			return target, nil
		}
		value.Set(reflect.MakeSlice(value.Type(), items.Len(), items.Len()))
		for i := 0; i < items.Len(); i++ {
			item, err := enumPresentationValue(value.Type().Elem(), items.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			value.Index(i).Set(item)
		}
		return target, nil
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
	case reflect.Slice, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	default:
		// Union codecs retain their existing owners.
		return nil, nil
	}
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil || enumeration == nil {
		return nil, err
	}
	target, err := enumPresentationType(enumeration)
	if err != nil {
		return nil, err
	}
	for source := field.Type; source.Kind() == reflect.Pointer; source = source.Elem() {
		target = reflect.PointerTo(target)
	}
	return &presentation{source: field.Type, target: target, enumeration: enumeration}, nil
}

// enumPresentationType uses the descriptor's prepared item owner; collection
// declarations are never parsed again to discover the presentation element.
func enumPresentationType(enumeration *choice.Enum) (reflect.Type, error) {
	if enumeration.CollectionItems() != nil {
		item := reflect.TypeFor[string]()
		if element := enumeration.CollectionElement(); element != nil {
			var err error
			item, err = enumPresentationType(element)
			if err != nil {
				return nil, err
			}
		}
		return reflect.SliceOf(item), nil
	}
	var target reflect.Type
	switch enumeration.WireType(false) {
	case "boolean":
		target = reflect.TypeFor[bool]()
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
	return target, nil
}

// opaqueCellContainer recognizes only containers without a static row contract.
// Scalar JSON/Text owners, including time and custom structs, retain their owner.
func opaqueCellContainer(typ reflect.Type) bool {
	seen := map[reflect.Type]bool{}
	for typ != nil {
		if seen[typ] {
			return false
		}
		seen[typ] = true
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			typ = typ.Elem()
		case reflect.Map, reflect.Interface:
			return true
		default:
			return false
		}
	}
	return false
}

func canonicalFieldType(typ reflect.Type, indexes []int) reflect.Type {
	for _, index := range indexes {
		for typ != nil && (typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
			typ = typ.Elem()
		}
		if typ == nil || typ.Kind() != reflect.Struct || index >= typ.NumField() {
			return nil
		}
		typ = typ.Field(index).Type
	}
	return typ
}
