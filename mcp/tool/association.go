package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/associationtag"
	intent "github.com/viant/datly/mcp/internal/association"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/tagly/format"
)

// The route plan keeps the typed path alongside all accepted public spellings.
// An array step contributes its concrete index only when translating a call.
type associationStep struct {
	names     []string
	foldNames []string
	field     string
	array     bool
}
type associationRule struct {
	parents  []associationStep
	field    associationStep
	holder   reflect.Type
	member   string
	idType   reflect.Type
	add, del string
}

type hiddenAssociationMember struct {
	parents []associationStep
	member  associationStep
}

func (h hiddenAssociationMember) reject(path string, obj map[string]any) (map[string]any, error) {
	for name := range obj {
		if h.member.matches(name) {
			return nil, fmt.Errorf("MCP association input %q is not public", appendFieldPath(path, name))
		}
	}
	return nil, nil
}

func associationArgumentStep(arg Argument) associationStep {
	step := associationStep{names: append([]string{arg.publicName}, arg.aliases...)}
	if arg.anonymousGoName != "" {
		step.names = append(step.names, arg.anonymousGoName)
	}
	return step
}

func compileAssociations(hidden *[]hiddenAssociationMember, input reflect.Type, args []Argument, s *schema.ToolInputSchema, format string) ([]associationRule, error) {
	properties := map[string]any{}
	var reserved associationStep
	for key, value := range s.Properties {
		properties[key] = value
		reserved.names = append(reserved.names, key)
	}
	for _, arg := range args {
		for _, alias := range arg.aliases {
			reserved.names = append(reserved.names, alias)
		}
	}
	root := map[string]any{"properties": properties, "required": s.Required}
	if extras, ok := s.AdditionalProperties.(map[string]any); ok {
		maps.Copy(root, extras)
	}
	var rules []associationRule
	for _, arg := range args {
		if arg.sourceKind != "body" || arg.customJSONBody || arg.wireType != nil || arg.wireSchema != nil {
			continue
		}
		holder := input
		var field reflect.StructField
		var nativePath []string
		for i, index := range arg.associationIndex {
			holder = indirectAssociationType(holder)
			if holder == nil || holder.Kind() != reflect.Struct || index >= holder.NumField() {
				field = reflect.StructField{}
				break
			}
			field = holder.Field(index)
			nativePath = append(nativePath, field.Name)
			if i+1 < len(arg.associationIndex) {
				holder = field.Type
			}
		}
		if field.Type == nil || !hasAssociationTag(field, map[reflect.Type]bool{}) {
			continue
		}
		// Anonymous body fields carry effective tags on the individually projected field.
		if field.Tag.Get("association") != arg.associationTag.Get("association") {
			field.Tag = ""
		}
		step := associationArgumentStep(arg)
		step.field = strings.Join(nativePath, ".")
		node := s.Properties[arg.publicName]
		if err := collectAssociationRules(hidden, &rules, nil, step, holder, field, arg.sourceType, representationVisibility(arg.representation, format), root, &reserved, node); err != nil {
			return nil, err
		}
	}
	clear(s.Properties)
	for key, value := range properties {
		s.Properties[key] = value.(map[string]any)
	}
	if len(*hidden) > 0 {
		s.Required = schemaRequiredNames(root["required"])
		delete(root, "required")
		delete(root, "properties")
		if len(root) > 0 {
			s.AdditionalProperties = root
		}
	}
	return rules, nil
}

// replaceAssociationMember projects a native input (and its accepted aliases)
// as primitive delta fields. Keep requiredness on the public choices only.
func replaceAssociationMember(parent map[string]any, step associationStep, published ...string) {
	props := parent["properties"].(map[string]any)
	for name := range props {
		if step.matches(name) {
			delete(props, name)
		}
	}
	required := schemaRequiredNames(parent["required"])
	needed := slices.ContainsFunc(required, step.matches)
	if needed {
		parent["required"] = slices.DeleteFunc(slices.Clone(required), step.matches)
	}
	all, _ := parent["allOf"].([]any)
	all = slices.DeleteFunc(all, func(clause any) bool {
		group, _ := clause.(map[string]any)
		options, _ := group["anyOf"].([]any)
		if len(group) != 1 || len(options) == 0 {
			return false
		}
		for _, option := range options {
			choice, _ := option.(map[string]any)
			names := schemaRequiredNames(choice["required"])
			if len(names) != 1 || !step.matches(names[0]) {
				return false
			}
		}
		needed = true
		return true
	})
	if needed && len(published) > 0 {
		var choices []any
		for _, name := range published {
			choices = append(choices, map[string]any{"required": []string{name}})
		}
		all = append(all, map[string]any{"anyOf": choices})
	}
	if len(all) > 0 {
		parent["allOf"] = all
	} else {
		delete(parent, "allOf")
	}
}

func nativeAssociationField(owner reflect.Type, index []int) (reflect.StructField, bool) {
	var field reflect.StructField
	for _, i := range index {
		owner = indirectAssociationType(owner)
		if owner == nil || owner.Kind() != reflect.Struct || i >= owner.NumField() {
			return field, false
		}
		field = owner.Field(i)
		owner = field.Type
	}
	return field, len(index) > 0
}

func indirectAssociationType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func hasAssociationTag(f reflect.StructField, seen map[reflect.Type]bool) bool {
	if _, ok := f.Tag.Lookup("association"); ok {
		return true
	}
	t := f.Type
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || seen[t] {
		return false
	}
	seen[t] = true
	for i := 0; i < t.NumField(); i++ {
		if child := t.Field(i); child.IsExported() && hasAssociationTag(child, seen) {
			return true
		}
	}
	return false
}

func collectAssociationRules(hidden *[]hiddenAssociationMember, rules *[]associationRule, parents []associationStep, step associationStep, holder reflect.Type, f reflect.StructField, t reflect.Type, visibility fieldname.Visibility, parent map[string]any, reserved *associationStep, node map[string]any) error {
	if node == nil {
		return nil
	}
	desc, err := associationtag.Parse(f)
	if err != nil {
		return err
	}
	native := f.Type
	for native.Kind() == reflect.Pointer || native.Kind() == reflect.Slice || native.Kind() == reflect.Array {
		native = native.Elem()
	}
	path := append(slices.Clone(parents), step)
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Kind() != reflect.Pointer {
			path = append(path, associationStep{array: true})
		}
		t = t.Elem()
		if visibility.Representation != nil {
			visibility.Representation = visibility.Representation.Elem()
		}
	}
	if native.Kind() != reflect.Struct || t.Kind() != reflect.Struct || fieldname.CustomJSON(t) {
		return nil
	}
	object := schemaObjectNode(node)
	children, _ := object["properties"].(map[string]any)

	entries, err := fieldname.Fields(t, visibility)
	if err != nil {
		return err
	}
	if desc != nil {
		properties := parent["properties"].(map[string]any)
		*hidden = append(*hidden, hiddenAssociationMember{parents: slices.Clone(parents), member: step})
		for _, entry := range entries {
			if !slices.Equal(entry.Field.Index, desc.Member.Index) {
				continue
			}
			memberSchema, _ := children[entry.Name.Public].(map[string]any)
			if memberSchema == nil {
				continue
			}
			rule := associationRule{parents: slices.Clone(parents), field: step, holder: holder, member: entry.Name.Public, idType: indirectAssociationType(desc.Member.Type), add: desc.Collection + "ToBeAdded", del: desc.Collection + "ToBeDeleted"}
			item := cloneSchemaMap(memberSchema)
			removeSchemaNull(item)
			for _, name := range []string{rule.add, rule.del} {
				if reserved.matches(name) || properties[name] != nil {
					return fmt.Errorf("association companion %q conflicts with an existing property", name)
				}
				properties[name] = map[string]any{"type": "array", "items": cloneSchemaMap(item)}
				reserved.names = append(reserved.names, name)
			}
			replaceAssociationMember(parent, step, rule.add, rule.del)
			*rules = append(*rules, rule)
			return nil
		}
		replaceAssociationMember(parent, step)
		return nil
	}
	var occupied associationStep
	for _, entry := range entries {
		step, err := associationEntryStep(entry)
		if err != nil {
			return err
		}
		occupied.names = append(occupied.names, step.names...)
		occupied.foldNames = append(occupied.foldNames, step.foldNames...)
	}
	for _, entry := range entries {
		child, _ := children[entry.Name.Public].(map[string]any)
		if child == nil {
			continue
		}
		// Effective reflection can replace a Go type or rename its fields.
		// Field indexes retain the executable type's identity across overlays.
		nativeField, ok := nativeAssociationField(native, entry.Field.Index)
		if !ok || !hasAssociationTag(nativeField, map[reflect.Type]bool{}) {
			continue
		}
		if nativeField.Tag.Get("association") != entry.Field.Tag.Get("association") {
			nativeField.Tag = ""
		}
		v := visibility
		v.Representation = entry.Representation
		step, err := associationEntryStep(entry)
		if err != nil {
			return err
		}
		step.field = nativeField.Name
		if err := collectAssociationRules(hidden, rules, path, step, native, nativeField, entry.Field.Type, v, object, &occupied, child); err != nil {
			return err
		}
	}
	return nil
}

// AssociationScope prepares a tool invocation without a protocol handler and
// returns the same target-bound context used by the native component invoker.
func (p *Plan) AssociationScope(ctx context.Context, arguments map[string]any) (context.Context, *requestprovider.Scope, error) {
	if p == nil {
		return ctx, nil, fmt.Errorf("MCP tool plan is required")
	}
	ctx = intent.New(ctx, p.target)
	scope, err := p.scope(ctx, arguments)
	if err == nil {
		ctx = intent.TransactionContext(ctx, p.target)
	}
	return ctx, scope, err
}
func (p *Plan) translateAssociations(edits intent.Edits, args map[string]any) (map[string]any, error) {
	for _, rule := range p.associations {
		value, err := transformAssociationPath(rule.parents, "", args, func(path string, obj map[string]any) (map[string]any, error) { return rule.translate(edits, path, obj) })
		if err != nil {
			return nil, err
		}
		if value != nil {
			args = value.(map[string]any)
		}
	}
	return args, nil
}

func appendFieldPath(parent, field string) string {
	if parent == "" {
		return field
	}
	return parent + "." + field
}

func associationEntryStep(entry fieldname.Entry) (associationStep, error) {
	step := associationStep{names: append([]string{entry.Name.Public, entry.JSONName, entry.Field.Name}, entry.Name.Aliases...), foldNames: []string{entry.JSONName}, field: entry.Field.Name}
	policy, err := format.Parse(entry.Field.Tag)
	if err != nil {
		return step, err
	}
	if policy.InputNames != "" {
		aliases := strings.Split(policy.InputNames, "|")
		step.names = append(step.names, aliases...)
		if policy.InputNamesCase == "fold" {
			step.foldNames = append(step.foldNames, aliases...)
		}
	}
	return step, nil
}

func (s associationStep) matches(name string) bool {
	if slices.Contains(s.names, name) {
		return true
	}
	for _, fold := range s.foldNames {
		if strings.EqualFold(name, fold) {
			return true
		}
	}
	return false
}

func transformAssociationPath(parents []associationStep, path string, value any, translate func(string, map[string]any) (map[string]any, error)) (any, error) {
	if len(parents) != 0 {
		step := parents[0]
		if step.array {
			list := reflect.ValueOf(value)
			if !list.IsValid() || (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) {
				return nil, nil
			}
			// Byte buffers and raw JSON have a different decoding authority.
			if list.Type().Elem().Kind() == reflect.Uint8 {
				return nil, nil
			}
			var values []any
			for i := 0; i < list.Len(); i++ {
				updated, err := transformAssociationPath(parents[1:], path+"["+strconv.Itoa(i)+"]", list.Index(i).Interface(), translate)
				if err != nil {
					return nil, err
				}
				if updated != nil {
					if values == nil {
						values = make([]any, list.Len())
						for j := range values {
							values[j] = list.Index(j).Interface()
						}
					}
					values[i] = updated
				}
			}
			if values == nil {
				return nil, nil
			}
			return values, nil
		}
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, nil
		}

		var result map[string]any
		var supplied int
		for name, child := range obj {
			if !step.matches(name) {
				continue
			}
			supplied++
			v, err := transformAssociationPath(parents[1:], appendFieldPath(path, step.field), child, translate)
			if err != nil {
				return nil, err
			}
			if v != nil {
				if result == nil {
					result = maps.Clone(obj)
				}
				result[name] = v
			}
		}
		if result != nil {
			if supplied > 1 {
				return nil, fmt.Errorf("conflicting MCP names for association parent %q", step.names[0])
			}
			return result, nil
		}
		return nil, nil
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, nil
	}
	result, err := translate(path, obj)
	if result == nil {
		return nil, err
	}
	return result, err
}

func (rule associationRule) translate(edits intent.Edits, path string, obj map[string]any) (map[string]any, error) {
	add, haveAdd := obj[rule.add]
	del, haveDel := obj[rule.del]
	if !haveAdd && !haveDel {
		return nil, nil
	}
	delta := intent.Delta{}
	rows := []any{}
	for _, operation := range []struct {
		name    string
		value   any
		present bool
		ids     *map[any]struct{}
	}{{rule.add, add, haveAdd, &delta.Added}, {rule.del, del, haveDel, &delta.Deleted}} {
		if !operation.present {
			continue
		}
		list := reflect.ValueOf(operation.value)
		if !list.IsValid() || (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || (list.Kind() == reflect.Slice && list.IsNil()) {
			return nil, fmt.Errorf("%s must be an array", operation.name)
		}
		*operation.ids = make(map[any]struct{}, list.Len())
		for i := 0; i < list.Len(); i++ {
			id, err := associationID(list.Index(i).Interface(), rule.idType)
			if err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", operation.name, i, err)
			}
			if _, seen := (*operation.ids)[id]; seen {
				continue
			}
			if _, added := delta.Added[id]; added && operation.name == rule.del {
				return nil, fmt.Errorf("overlapping association ID in %s", operation.name)
			}
			(*operation.ids)[id] = struct{}{}
			rows = append(rows, map[string]any{rule.member: id})
		}
	}
	fieldPath := appendFieldPath(path, rule.field.field)
	fieldName := rule.field.field[strings.LastIndex(rule.field.field, ".")+1:]
	edits.Set(rule.holder, fieldName, fieldPath, delta)
	result := maps.Clone(obj)
	delete(result, rule.add)
	delete(result, rule.del)
	result[rule.field.names[0]] = rows
	return result, nil
}

func associationID(value any, native reflect.Type) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("null association ID")
	}
	v := reflect.ValueOf(value)
	result := reflect.New(native).Elem()
	if native.Kind() == reflect.String {
		if v.Kind() != reflect.String || v.Type() == reflect.TypeFor[json.Number]() {
			return nil, fmt.Errorf("expected string ID")
		}
		result.SetString(v.String())
		return result.Interface(), nil
	}
	var decimal string
	if number, ok := value.(json.Number); ok {
		decimal = number.String()
	} else {
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			decimal = strconv.FormatInt(v.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			decimal = strconv.FormatUint(v.Uint(), 10)
		case reflect.Float64, reflect.Float32:
			number := v.Float()
			if math.Trunc(number) != number || math.Abs(number) >= 1<<53 {
				return nil, fmt.Errorf("expected an exactly representable integer ID")
			}
			decimal = strconv.FormatFloat(number, 'f', 0, 64)
		default:
			return nil, fmt.Errorf("expected integer ID")
		}
	}
	if native.Kind() >= reflect.Int && native.Kind() <= reflect.Int64 {
		number, err := strconv.ParseInt(decimal, 10, native.Bits())
		if err != nil {
			return nil, fmt.Errorf("invalid %s ID", native)
		}
		result.SetInt(number)
	} else {
		number, err := strconv.ParseUint(decimal, 10, native.Bits())
		if err != nil {
			return nil, fmt.Errorf("invalid %s ID", native)
		}
		result.SetUint(number)
	}
	return result.Interface(), nil
}
