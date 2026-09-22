package report

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func (c *inputCompiler) compileLayout(typeOf reflect.Type, descriptor *x.Type) (*inputCompilation, error) {
	result := &inputCompilation{typeOf: typeOf, descriptor: descriptor}
	sections := []struct {
		name   string
		fields []field
		target *[]selection
	}{
		{name: c.metadata.inputLayout.Dimensions, fields: c.metadata.dimensions, target: &result.dimensions},
		{name: c.metadata.inputLayout.Measures, fields: c.metadata.measures, target: &result.measures},
	}
	for _, section := range sections {
		sectionField, err := xshape.Linked(typeOf).StructField(section.name)
		if err != nil || normalizeStructType(sectionField.Type) == nil {
			return nil, fmt.Errorf("report input %s requires struct field %s", typeOf, section.name)
		}
		sectionType := normalizeStructType(sectionField.Type)
		for _, item := range section.fields {
			selected, ok := typecatalog.FieldByName(sectionType, item.fieldName)
			if !ok || dereference(selected.Type).Kind() != reflect.Bool {
				return nil, fmt.Errorf("report input %s.%s requires bool field %s", typeOf, section.name, item.fieldName)
			}
			*section.target = append(*section.target, selection{name: item.name, index: joinIndex(sectionField.Index, selected.Index)})
		}
	}
	if err := c.compileFilterLayout(typeOf, result); err != nil {
		return nil, err
	}
	var err error
	if result.orderIndex, err = exactFieldIndex(typeOf, c.metadata.inputLayout.OrderBy, reflect.TypeOf([]string{})); err != nil {
		return nil, err
	}
	if result.limitIndex, err = exactFieldIndex(typeOf, c.metadata.inputLayout.Limit, reflect.TypeOf((*int)(nil))); err != nil {
		return nil, err
	}
	if result.offsetIndex, err = exactFieldIndex(typeOf, c.metadata.inputLayout.Offset, reflect.TypeOf((*int)(nil))); err != nil {
		return nil, err
	}
	for _, name := range []string{
		c.metadata.inputLayout.Dimensions, c.metadata.inputLayout.Measures, c.metadata.inputLayout.Filters,
		c.metadata.inputLayout.OrderBy, c.metadata.inputLayout.Limit, c.metadata.inputLayout.Offset,
	} {
		param := bodyParam(name, typeOf)
		if name == c.metadata.inputLayout.Filters {
			param.WireSchemas = filterWireSchemas(name, c.metadata.filters)
		}
		result.params = append(result.params, param)
	}
	return result, nil
}

func (c *inputCompiler) compileFilterLayout(typeOf reflect.Type, result *inputCompilation) error {
	section, err := xshape.Linked(typeOf).StructField(c.metadata.inputLayout.Filters)
	if err != nil || normalizeStructType(section.Type) == nil {
		return fmt.Errorf("report input %s requires struct field %s", typeOf, c.metadata.inputLayout.Filters)
	}
	filterType := normalizeStructType(section.Type)
	for _, item := range c.metadata.filters {
		field, ok := typecatalog.FieldByName(filterType, item.name)
		if !ok {
			return fmt.Errorf("report input %s.%s requires field %s", typeOf, c.metadata.inputLayout.Filters, item.fieldName)
		}
		sourceType := item.contract.SourceType()
		if sourceType == nil {
			sourceType = item.contract.DestinationType()
		}
		if !filterFieldCompatible(field.Type, sourceType) {
			return fmt.Errorf("report filter %s.%s type %s cannot provide source type %s", c.metadata.inputLayout.Filters, item.fieldName, field.Type, sourceType)
		}
		binding := item.contract.Binding()
		result.filters = append(result.filters, filter{
			name: item.name, location: binding.Location, sourceType: sourceType, index: joinIndex(section.Index, field.Index),
		})
	}
	return nil
}
