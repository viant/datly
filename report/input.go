package report

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type inputCompilation struct {
	typeOf      reflect.Type
	descriptor  *x.Type
	params      []*spec.Parameter
	dimensions  []selection
	measures    []selection
	filters     []filter
	orderIndex  []int
	limitIndex  []int
	offsetIndex []int
}

type inputCompiler struct {
	metadata    *metadata
	component   *spec.Component
	typeName    string
	packagePath string
	types       *typecatalog.Catalog
	authority   typecatalog.Authority
}

func (c *inputCompiler) compile() (*inputCompilation, error) {
	if c.metadata.settings.LinkedInputType != "" {
		return c.compileLinked()
	}
	return c.compileGenerated()
}

func (c *inputCompiler) compileGenerated() (*inputCompilation, error) {
	dimensions, err := sectionType(c.metadata.dimensions)
	if err != nil {
		return nil, err
	}
	measures, err := sectionType(c.metadata.measures)
	if err != nil {
		return nil, err
	}
	filters, err := c.filterType()
	if err != nil {
		return nil, err
	}
	fields := []xshape.RuntimeField{
		inputField(c.metadata.inputLayout.Dimensions, dimensions, "Selected grouping dimensions"),
		inputField(c.metadata.inputLayout.Measures, measures, "Selected aggregate measures"),
		inputField(c.metadata.inputLayout.Filters, filters, "Report filters"),
		inputField(c.metadata.inputLayout.OrderBy, reflect.TypeOf([]string{}), "Grouped result ordering"),
		inputField(c.metadata.inputLayout.Limit, reflect.TypeOf((*int)(nil)), "Maximum grouped rows"),
		inputField(c.metadata.inputLayout.Offset, reflect.TypeOf((*int)(nil)), "Grouped row offset"),
	}
	typeOf, err := (xshape.Runtime{}).Struct(fields)
	if err != nil {
		return nil, err
	}
	descriptor, err := (xshape.Runtime{}).Synthetic(c.packagePath, c.typeName, typeOf)
	if err != nil {
		return nil, err
	}
	return c.compileLayout(typeOf, descriptor)
}

func (c *inputCompiler) compileLinked() (*inputCompilation, error) {
	if c.types == nil {
		return nil, fmt.Errorf("linked report input %q requires a type catalog", c.metadata.settings.LinkedInputType)
	}
	resolver, err := typecatalog.NewResolver(c.types, c.authority, c.resolutionContext())
	if err != nil {
		return nil, err
	}
	descriptor, err := resolver.Descriptor(c.metadata.settings.LinkedInputType)
	if err != nil {
		return nil, err
	}
	if descriptor == nil || descriptor.Type == nil {
		return nil, fmt.Errorf("linked report input %q has no runtime type", c.metadata.settings.LinkedInputType)
	}
	typeOf := normalizeStructType(descriptor.Type)
	if typeOf == nil {
		return nil, fmt.Errorf("linked report input %q must be a struct", c.metadata.settings.LinkedInputType)
	}
	return c.compileLayout(typeOf, descriptor)
}
