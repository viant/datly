package contract

import (
	"reflect"
	"strings"

	"github.com/viant/tagly/tags"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

// Project applies only changed Name/Tag metadata to an existing transport shape.
// mapPath maps a projected canonical field path to the linked contract path;
// empty means this field belongs to a different transport owner (e.g. selectors).
func Project(typ reflect.Type, source *shape.Representation, mapPath func(string) string) (*shape.Representation, error) {
	if source == nil {
		return nil, nil
	}
	walker, err := shape.CompileRewrite(typ)
	if err != nil {
		return nil, err
	}
	return walker.RewriteRepresentation(func(path string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		full := field.Name
		if path != "" {
			full = path + "." + full
		}
		canonical := mapPath(strings.ReplaceAll(strings.ReplaceAll(full, "[]", ""), "{}", ""))
		if canonical == "" {
			return nil, nil
		}
		original, selected, ok := projectedField(source, canonical)
		if !ok {
			return nil, nil
		}
		if original.Name == selected.Name && original.Tag == selected.Tag {
			return nil, nil
		}
		result := *field
		if selected.Name != original.Name {
			result.Name = selected.Name
		}
		result.Tag, err = projectTags(field.Tag, original.Tag, selected.Tag)
		if err != nil {
			return nil, err
		}
		return &result, nil
	})
}
func projectedField(metadata *shape.Representation, path string) (reflect.StructField, reflect.StructField, bool) {
	var original, selected reflect.StructField
	for _, name := range strings.Split(path, ".") {
		for metadata != nil && (metadata.Original().Kind() == reflect.Pointer || metadata.Original().Kind() == reflect.Slice || metadata.Original().Kind() == reflect.Array || metadata.Original().Kind() == reflect.Map) {
			metadata = metadata.Elem()
		}
		if metadata == nil || metadata.Original().Kind() != reflect.Struct {
			return original, selected, false
		}
		var ok bool
		original, ok = metadata.Original().FieldByName(name)
		if !ok {
			return original, selected, false
		}
		for _, index := range original.Index {
			for metadata.Original().Kind() == reflect.Pointer {
				metadata = metadata.Elem()
			}
			selected = metadata.Field(index)
			metadata = metadata.FieldShape(index)
		}
	}
	return original, selected, true
}
func projectTags(projected, original, selected reflect.StructTag) (reflect.StructTag, error) {
	before, err := tags.Parse(string(original))
	if err != nil {
		return "", err
	}
	after, err := tags.Parse(string(selected))
	if err != nil {
		return "", err
	}
	base, err := tags.Parse(string(projected))
	if err != nil {
		return "", err
	}
	oldValues, newValues := map[string][]string{}, map[string][]string{}
	for _, tag := range before {
		oldValues[tag.Name] = append(oldValues[tag.Name], string(tag.Values))
	}
	for _, tag := range after {
		newValues[tag.Name] = append(newValues[tag.Name], string(tag.Values))
	}
	changed := map[string]bool{}
	for key, values := range oldValues {
		if !reflect.DeepEqual(values, newValues[key]) {
			changed[key] = true
		}
	}
	for key, values := range newValues {
		if !reflect.DeepEqual(values, oldValues[key]) {
			changed[key] = true
		}
	}
	result := make(tags.Tags, 0, len(base))
	for _, tag := range base {
		if !changed[tag.Name] {
			result = append(result, tag)
		}
	}
	for _, tag := range after {
		if changed[tag.Name] {
			result = append(result, tag)
		}
	}
	value := reflect.StructTag(result.Literal())
	beforeRequired, err := bindingRequired(original)
	if err != nil {
		return "", err
	}
	afterRequired, err := bindingRequired(selected)
	if err != nil {
		return "", err
	}
	if beforeRequired != afterRequired {
		field := &xcontract.Field{Name: "Field", Tag: value}
		if afterRequired {
			field = field.Required()
		} else {
			field = field.Optional()
		}
		return field.Tag, field.Err()
	}
	return value, nil
}

// LookupField returns effective field annotations at a canonical linked path.
func LookupField(metadata *shape.Representation, path string) (reflect.StructField, bool) {
	if metadata == nil {
		return reflect.StructField{}, false
	}
	_, field, ok := projectedField(metadata, path)
	return field, ok
}

// ProjectTags retains transport-owned annotations except keys the policy edited.
func ProjectTags(projected, original, selected reflect.StructTag) (reflect.StructTag, error) {
	return projectTags(projected, original, selected)
}

func bindingRequired(tag reflect.StructTag) (bool, error) {
	if value, ok := tag.Lookup("required"); ok {
		return value == "" || value == "true", nil
	}
	for _, key := range []string{"parameter", "bind"} {
		raw, ok := tag.Lookup(key)
		if !ok {
			continue
		}
		name, rest := tags.Values(raw).Name()
		if strings.Contains(name, "=") {
			rest = tags.Values(raw)
		}
		required := false
		err := rest.MatchRawPairs(func(key, value string) error {
			if key == "required" {
				required = value == "" || value == "true" || value == "1"
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		if required {
			return true, nil
		}
	}
	return false, nil
}
