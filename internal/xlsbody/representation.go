package xlsbody

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/tagly/tags"
	"github.com/viant/x/shape"
)

// Decoder retains the canonical workbook method and its request context.
type Decoder struct{ Context context.Context }

func (d Decoder) Decode(raw []byte, target any) error { return Unmarshal(d.Context, raw, target) }

// UnmarshalWithRepresentation accepts the existing storage-enum presentation
// rewrite only. Workbook codecs do not implement arbitrary JSON field policies.
// Unsupported restrictions fail before the canonical workbook decoder runs.
func (d Decoder) UnmarshalWithRepresentation(raw []byte, target any, metadata *shape.Representation) error {
	if err := d.Context.Err(); err != nil {
		return err
	}
	if metadata != nil && reflect.TypeOf(target) != reflect.PointerTo(metadata.Original()) {
		return fmt.Errorf("XLSX representation target does not match canonical body")
	}
	if err := validateRepresentation(metadata, make(map[*shape.Representation]bool)); err != nil {
		return err
	}
	return d.Decode(raw, target)
}

func validateRepresentation(metadata *shape.Representation, visited map[*shape.Representation]bool) error {
	if metadata == nil || !metadata.Changed() || visited[metadata] {
		return nil
	}
	visited[metadata] = true
	if err := validateRepresentation(metadata.Elem(), visited); err != nil {
		return err
	}
	original := metadata.Original()
	for i := 0; i < metadata.NumField(); i++ {
		before, after := original.Field(i), metadata.Field(i)
		if before.Name != after.Name || before.Type != after.Type || before.Anonymous != after.Anonymous || before.PkgPath != after.PkgPath || before.Offset != after.Offset || !reflect.DeepEqual(before.Index, after.Index) {
			return fmt.Errorf("XLSX representation changes canonical field %s", before.Name)
		}
		if before.Tag != after.Tag {
			if before.Tag.Get("mcpEnum") != "true" {
				return fmt.Errorf("XLSX representation does not support field policy on %s", before.Name)
			}
			parsed, err := tags.Parse(string(before.Tag))
			if err != nil {
				return err
			}
			expected := tags.Tags{}
			removed := false
			for _, tag := range parsed {
				if storageEnumTag(tag.Name) {
					removed = true
					continue
				}
				expected = append(expected, tag)
			}
			actual, err := tags.Parse(string(after.Tag))
			if err != nil {
				return err
			}
			if !removed || !reflect.DeepEqual(expected, actual) {
				return fmt.Errorf("XLSX representation does not support field policy on %s", before.Name)
			}
		}
		if err := validateRepresentation(metadata.FieldShape(i), visited); err != nil {
			return err
		}
	}
	return nil
}

func storageEnumTag(name string) bool {
	switch name {
	case "choice", "enumAccept", "enumStyle", "enumCase", "enumTrim", "enumEmpty", "enumZero", "enumNull", "enumInteger", "enumUnknownInteger", "enumUnknownString", "enumAlias", "enumOutputAlias", "enumOutputUnknown":
		return true
	}
	return false
}
