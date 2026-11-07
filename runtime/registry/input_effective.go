package registry

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
)

// MetadataFields projects effective Name/Tag metadata without replacing the
// canonical executable input plan. Field positions retain canonical identity.
func (c *RouteInputContract) MetadataFields(effective reflect.Type, projected []InputField) ([]InputField, error) {
	return c.metadataFields(effective, nil, projected)
}

// MetadataRepresentation retains canonical storage while overlaying public
// field metadata. Only fields owned by this route are adjusted.
func (c *RouteInputContract) MetadataRepresentation(metadata *xshape.Representation, projected []InputField) ([]InputField, error) {
	if metadata != nil && metadata.Original() != c.Type() {
		return nil, fmt.Errorf("input metadata does not match linked type")
	}
	if metadata != nil && !metadata.Changed() {
		metadata = nil
	}
	return c.metadataFields(c.Type(), metadata, projected)
}
func (c *RouteInputContract) metadataFields(effective reflect.Type, metadata *xshape.Representation, projected []InputField) ([]InputField, error) {

	fields := append([]InputField(nil), projected...)
	if projected == nil {
		fields = c.Fields()
	}
	if effective == c.Type() && metadata == nil {
		return fields, nil
	}
	for i := range fields {
		f := &fields[i]
		if f.owner != c.Type() || f.origin != c.Route() {
			continue
		}
		original, err := f.StructField()
		if err != nil {
			return nil, err
		}
		parent := effective
		currentShape := metadata
		var projected reflect.StructField
		var path []string
		for _, index := range original.Index {
			for parent.Kind() == reflect.Pointer {
				parent = parent.Elem()
				if currentShape != nil {
					currentShape = currentShape.Elem()
				}
			}
			if parent.Kind() != reflect.Struct || index >= parent.NumField() {
				return nil, fmt.Errorf("effective input field %s changed structure", f.path)
			}
			projected = parent.Field(index)
			if currentShape != nil {
				projected = currentShape.Field(index)
				currentShape = currentShape.FieldShape(index)
			}
			path = append(path, projected.Name)
			parent = projected.Type
		}
		if metadata != nil {
			projected.Index = append([]int(nil), original.Index...)
			copy := projected
			f.metadataField = &copy
			f.ownerRepresentation = metadata
			if currentShape != nil && currentShape.Changed() {
				f.representation = currentShape
			}
		}
		f.owner = effective
		f.path = strings.Join(path, ".")
		f.destinationType = projected.Type
		f.binding = cloneBindingSpec(f.binding)
		// Codec source representations remain codec-owned. Ordinary body/parameter
		// shapes use the retagged destination as their effective wire source.
		if f.binding.Transformer == nil && f.binding.SourceType == original.Type {
			f.binding.SourceType = projected.Type
		}
		if original.Name != projected.Name && f.binding.Name == original.Name {
			f.binding.Name = projected.Name
		}
		if param, ok := f.binding.Extension.(*spec.Parameter); ok && param != nil {
			copy := *param
			copy.Tag = string(projected.Tag)
			if value, ok := projected.Tag.Lookup("description"); ok {
				copy.Description = value
			}
			if value, ok := projected.Tag.Lookup("example"); ok {
				copy.Example = value
			}
			f.binding.Extension = &copy
		}
		oldBinding, _, err := bindly.BindingSpecFromField(original)
		if err != nil {
			return nil, err
		}
		newBinding, _, err := bindly.BindingSpecFromField(projected)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(oldBinding.Required, newBinding.Required) {
			flag := newBinding.Required != nil && *newBinding.Required
			f.binding.Required, f.effectiveRequired = &flag, &flag
		}
		for _, key := range []string{"required", "optional"} {
			value, ok := projected.Tag.Lookup(key)
			oldValue, oldOK := original.Tag.Lookup(key)
			if ok == oldOK && value == oldValue {
				continue
			}
			flag := false
			if ok {
				flag, err = strconv.ParseBool(value)
				if err != nil {
					return nil, err
				}
			}
			if key == "optional" {
				if !ok {
					continue
				}
				flag = !flag
			}
			f.binding.Required = &flag
			f.effectiveRequired = &flag
		}
		if original.Tag != projected.Tag {
			f.binding, err = EnumBinding(f.binding, projected)
			if err != nil {
				return nil, err
			}
		}
		enumeration, err := choice.ParseEnum(projected.Tag, projected.Type)
		if err != nil {
			return nil, err
		}
		if enumeration != nil {
			f.wireSchema = &spec.WireSchema{Type: enumeration.WireType(true), Enum: enumeration.WireValues(true), Nullable: projected.Type.Kind() == reflect.Pointer}
			continue
		}
		if !strings.Contains(string(original.Tag), "choice:") && !strings.Contains(string(projected.Tag), "choice:") {
			continue
		}
		oldChoices, err := choice.Tags(original.Tag)
		if err != nil {
			return nil, err
		}
		newChoices, err := choice.Tags(projected.Tag)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(oldChoices, newChoices) {
			f.wireSchema = f.wireSchema.Clone()
			if len(newChoices) == 0 {
				if f.wireSchema != nil {
					f.wireSchema.Enum = nil
				}
			} else {
				constraint, err := choice.Parse(projected.Tag, f.SourceType())
				if err != nil {
					return nil, err
				}
				if f.wireSchema == nil {
					f.wireSchema = &spec.WireSchema{}
				}
				f.wireSchema.Type = constraint.JSONType()
				f.wireSchema.Enum = constraint.WireValues()
			}
		}
	}
	return fields, nil
}
