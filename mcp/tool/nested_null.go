package tool

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	bindinput "github.com/viant/bindly/input"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/jsonx"
	xshape "github.com/viant/x/shape"
)

// This projection only checks the authored null policy. The supplied JSON
// still goes to the ordinary body provider for selection, conversion and binding.
type nestedNullPlan struct {
	shape   *nestedNullPlan
	matcher *format.InputNameMatcher
	fields  []nestedNullField
	kind    reflect.Kind
}

type nestedNullField struct {
	publicName string
	goName     string
	nonNull    bool
	child      *nestedNullPlan
}

func compileNestedNulls(argument Argument, caseFormat string) (*nestedNullPlan, error) {
	if argument.sourceKind != "body" || argument.customJSONBody || argument.bodyCodec || argument.bodyNullPolicy != "" ||
		argument.wireType != nil || wireSchemaReplacesShape(argument.wireSchema) || hasWireRepresentation(argument.wireSchemas) {
		return nil, nil
	}
	policy, err := jsonx.Compile(argument.bodyInputTag, argument.sourceType)
	if err != nil || policy.Active() {
		return nil, err
	}

	type visitKey struct {
		typ      reflect.Type
		metadata *xshape.Representation
	}
	seen := map[visitKey]*nestedNullPlan{}
	hasPolicy := false
	var compile func(reflect.Type, *xshape.Representation) (*nestedNullPlan, error)
	compile = func(typ reflect.Type, metadata *xshape.Representation) (*nestedNullPlan, error) {
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
			if metadata != nil {
				metadata = metadata.Elem()
			}
		}
		if opaqueNullOwner(typ) {
			return nil, nil
		}
		key := visitKey{typ, metadata}
		if result, ok := seen[key]; ok {
			return result, nil
		}
		node := &nestedNullPlan{kind: typ.Kind()}
		switch typ.Kind() {
		case reflect.Array, reflect.Slice, reflect.Map:
			seen[key] = node
			if metadata != nil {
				metadata = metadata.Elem()
			}
			child, err := compile(typ.Elem(), metadata)
			node.shape = child
			return node, err
		case reflect.Struct:
			seen[key] = node
		default:
			return nil, nil
		}
		entries, err := fieldname.Fields(typ, representationVisibility(metadata, caseFormat))
		if err != nil {
			return nil, err
		}
		var names []format.InputName
		var policies []*format.Tag
		for _, entry := range entries {
			field := entry.Field
			tag, err := format.Parse(field.Tag)
			if err != nil {
				return nil, err
			}
			if tag.Ignore {
				continue
			}
			conversion, err := jsonx.Compile(field.Tag, field.Type)
			if err != nil {
				return nil, err
			}
			member := nestedNullField{publicName: entry.Name.Public, goName: field.Name}
			if !conversion.Active() && !opaqueNullOwner(field.Type) {
				member.nonNull = tag.Nullable != nil && !*tag.Nullable
				hasPolicy = hasPolicy || member.nonNull
				member.child, err = compile(field.Type, entry.Representation)
				if err != nil {
					return nil, err
				}
			}
			name := entry.JSONName
			if explicit, _, _ := strings.Cut(field.Tag.Get("json"), ","); explicit == "" && tag.Name != "" {
				name = tag.Name
			}
			name = caseFormattedFallback(field, name, tag.CaseFormat)
			names = append(names, format.InputName{Name: name, Aliases: tag.InputNames})
			policies = append(policies, tag)
			node.fields = append(node.fields, member)
		}
		node.matcher, err = format.NewInputNameMatcher(names, policies...)
		return node, err
	}
	result, err := compile(argument.sourceType, argument.representation)
	if !hasPolicy {
		return nil, nil
	}
	return result, err
}

func opaqueNullOwner(typ reflect.Type) bool {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	pointer := reflect.PointerTo(typ)
	return fieldname.CustomJSON(typ) ||
		typ.Implements(reflect.TypeFor[json.Marshaler]()) || pointer.Implements(reflect.TypeFor[json.Marshaler]()) ||
		typ.Implements(reflect.TypeFor[encoding.TextMarshaler]()) || pointer.Implements(reflect.TypeFor[encoding.TextMarshaler]())
}

func (n *nestedNullPlan) check(value any, path string, ancestors []format.InputScope) error {
	if n == nil {
		return nil
	}
	switch value := value.(type) {
	case []any:
		if n.kind == reflect.Array || n.kind == reflect.Slice {
			for i, item := range value {
				if err := n.shape.check(item, fmt.Sprintf("%s[%d]", path, i), ancestors); err != nil {
					return err
				}
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if n.kind == reflect.Map {
			for _, key := range keys {
				if err := n.shape.check(value[key], fmt.Sprintf("%s[%q]", path, key), ancestors); err != nil {
					return err
				}
			}
		}
		if n.kind == reflect.Struct {
			return n.checkFields(value, keys, path, ancestors)
		}
	}
	return nil
}

func (n *nestedNullPlan) checkFields(value map[string]any, keys []string, path string, ancestors []format.InputScope) error {
	members := make([]format.InputMember, len(keys))
	for i, key := range keys {
		members[i] = format.InputMember{Name: key, Kind: jsonInputKind(value[key])}
	}
	selected, err := n.matcher.SelectScoped(members, ancestors)
	if err != nil {
		// The native decoder owns errors in the input selection policy.
		return nil
	}
	scope := make(format.InputScope, len(n.fields))
	for _, field := range n.fields {
		scope[field.goName] = false
	}
	indexes := make([]int, len(keys))
	for i, key := range keys {
		indexes[i] = -1
		if index := n.matcher.Match(key); selected[i] && index >= 0 {
			indexes[i] = index
			scope[n.fields[index].goName] = true
		}
	}
	ancestors = append([]format.InputScope{scope}, ancestors...)
	for i, index := range indexes {
		if index < 0 {
			continue
		}
		field := n.fields[index]
		path := path + "." + field.publicName
		member := value[keys[i]]
		if field.nonNull && member == nil {
			return fmt.Errorf("MCP argument %q cannot be null: %w", path, &bindinput.Error{Cause: fmt.Errorf("authored nullable=false")})
		}
		if err := field.child.check(member, path, ancestors); err != nil {
			return err
		}
	}
	return nil
}

func jsonInputKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case string:
		return "string"
	case bool:
		return "boolean"
	default:
		return "number"
	}
}

func (a Argument) checkNestedNulls(value any) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode MCP argument %q: %w", a.publicName, err)
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := a.nestedNulls.check(decoded, a.publicName, nil); err != nil {
		return nil, err
	}
	return raw, nil
}
