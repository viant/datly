package tool

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/schema/fieldname"
)

// A primitive pair uses existing application-owned inputs. Translation does
// not publish a writer receipt or select a transaction mode.
type primitivePair struct {
	parents []associationStep
	dest    [2]associationStep
	names   [2]string
	idType  reflect.Type
}

func compilePrimitivePairs(hidden *[]hiddenAssociationMember, config []spec.MCPPrimitivePair, args []Argument, s *schema.ToolInputSchema, caseFormat string) ([]primitivePair, error) {
	var rules []primitivePair
	claimed := map[[2]string]bool{}
	for _, pair := range config {
		path := strings.Split(pair.Path, ".")
		if slices.Contains(path, "") || pair.Add == "" || pair.Remove == "" || pair.Add == pair.Remove {
			return nil, fmt.Errorf("invalid primitive pair at %q", pair.Path)
		}
		for _, name := range []string{pair.Add, pair.Remove} {
			key := [2]string{pair.Path, name}
			if claimed[key] {
				return nil, fmt.Errorf("primitive pair reuses %s.%s", pair.Path, name)
			}
			claimed[key] = true
		}
		for _, arg := range args {
			if arg.defaultPublicName != path[0] || arg.sourceKind != "body" || arg.customJSONBody || arg.wireType != nil || arg.wireSchema != nil {
				continue
			}
			node := s.Properties[arg.publicName]
			visibility := representationVisibility(arg.representation, caseFormat)
			rule := primitivePair{parents: []associationStep{associationArgumentStep(arg)}, names: [2]string{caseFormattedFallback(reflect.StructField{}, pair.Add+"ToBeAdded", caseFormat), caseFormattedFallback(reflect.StructField{}, pair.Add+"ToBeDeleted", caseFormat)}}
			visible, err := rule.compile(hidden, arg.sourceType, visibility, node, path[1:], pair)
			if err != nil {
				return nil, err
			}
			if visible {
				rules = append(rules, rule)
			}
		}
	}
	return rules, nil
}

func (r *primitivePair) compile(hidden *[]hiddenAssociationMember, t reflect.Type, visibility fieldname.Visibility, node map[string]any, path []string, pair spec.MCPPrimitivePair) (bool, error) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Kind() != reflect.Pointer {
			r.parents = append(r.parents, associationStep{array: true})
		}
		t = t.Elem()
		if visibility.Representation != nil {
			visibility.Representation = visibility.Representation.Elem()
		}
	}
	if t.Kind() != reflect.Struct || fieldname.CustomJSON(t) {
		return false, nil
	}
	object := schemaObjectNode(node)
	props, _ := object["properties"].(map[string]any)
	entries, err := fieldname.Fields(t, visibility)
	if err != nil {
		return false, err
	}
	var schemas [2]map[string]any
	var collisions []string
	for _, entry := range entries {
		name := entry.Name.Public
		defaultName, err := memberNameVisibility("lc").Resolve(entry.Field, entry.JSONName)
		if err != nil {
			return false, err
		}
		step, err := associationEntryStep(entry)
		if err != nil {
			return false, err
		}
		child, _ := props[name].(map[string]any)
		if len(path) > 0 {
			if defaultName.Public == path[0] && child != nil {
				r.parents = append(r.parents, step)
				visibility.Representation = entry.Representation
				return r.compile(hidden, entry.Field.Type, visibility, child, path[1:], pair)
			}
			continue
		}
		for i, alias := range r.names {
			if step.matches(alias) || props[alias] != nil {
				collisions = append(collisions, alias)
			}
			if defaultName.Public != [2]string{pair.Add, pair.Remove}[i] || child == nil {
				continue
			}
			typ := entry.Field.Type
			if typ.Kind() != reflect.Slice {
				return false, fmt.Errorf("primitive pair field %s must be an ID array", name)
			}
			id := typ.Elem()
			items, _ := child["items"].(map[string]any)
			expected := "integer"
			if id.Kind() == reflect.String {
				expected = "string"
			} else if id.Kind() < reflect.Int || id.Kind() > reflect.Uint64 {
				return false, fmt.Errorf("unsupported primitive pair ID %s", id)
			}
			if items["type"] != expected {
				return false, fmt.Errorf("primitive pair %s has incompatible effective item schema", name)
			}
			if r.idType != nil && r.idType != id {
				return false, fmt.Errorf("primitive pair ID types differ at %s", pair.Path)
			}
			r.idType = id
			r.dest[i], schemas[i] = step, child
		}
	}
	for i, destination := range r.dest {
		if len(destination.names) != 0 {
			*hidden = append(*hidden, hiddenAssociationMember{parents: slices.Clone(r.parents), member: destination})
			if schemas[1-i] == nil {
				replaceAssociationMember(object, destination)
			}
		}
	}
	if schemas[0] == nil || schemas[1] == nil {
		return false, nil
	}
	if len(collisions) > 0 {
		return false, fmt.Errorf("primitive companion %q conflicts with an existing property", collisions[0])
	}
	for i, alias := range r.names {
		props[alias] = cloneSchemaMap(schemas[i])
		removeSchemaNull(props[alias].(map[string]any))
		replaceAssociationMember(object, r.dest[i], alias)
	}
	return true, nil
}

func (r primitivePair) translate(_ string, obj map[string]any) (map[string]any, error) {
	var result map[string]any
	added := map[any]bool{}
	for i, alias := range r.names {
		value, present := obj[alias]
		if !present {
			continue
		}
		list := reflect.ValueOf(value)
		if !list.IsValid() || (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || (list.Kind() == reflect.Slice && list.IsNil()) {
			return nil, fmt.Errorf("%s must be an ID array", r.dest[i].names[0])
		}
		for j := 0; j < list.Len(); j++ {
			id, err := associationID(list.Index(j).Interface(), r.idType)
			if err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", r.dest[i].names[0], j, err)
			}
			if i == 0 {
				added[id] = true
			} else if added[id] {
				return nil, fmt.Errorf("overlapping primitive ID in %s", r.dest[i].names[0])
			}
		}
		if result == nil {
			result = maps.Clone(obj)
		}
		delete(result, alias)
		result[r.dest[i].names[0]] = value
	}
	return result, nil
}
