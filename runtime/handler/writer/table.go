package writer

import (
	"fmt"
	"strings"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/constant/identifier"
	"github.com/viant/datly/spec"
)

func resolvedWriterTable(component *spec.Component, table string, renderers ...identifier.Renderer) (string, error) {
	if !strings.Contains(table, "$") {
		return table, nil
	}
	// Bootstrap has applied the immutable instance to the component's declared
	// constants. Use the shared constant identifier renderer for DML too; invocation
	// values never select a writer's physical target.
	var defaults *constant.Values
	values, err := defaults.For(component)
	if err != nil {
		return "", err
	}
	renderer := identifier.Renderer{Values: values}
	if len(renderers) != 0 {
		renderer = renderers[0]
		renderer.Values = values
	}
	table, err = renderer.Metadata(table)
	if err != nil {
		return "", err
	}
	if strings.Contains(table, "$") {
		return "", fmt.Errorf("unresolved table identifier")
	}
	return table, nil
}

func resolveFieldReferences(component *spec.Component, field *Field, renderers ...identifier.Renderer) error {
	var err error
	if field.RefDB, err = resolvedWriterTable(component, field.RefDB, renderers...); err != nil {
		return fmt.Errorf("field %s reference schema: %w", field.Name, err)
	}
	if field.RefTable, err = resolvedWriterTable(component, field.RefTable, renderers...); err != nil {
		return fmt.Errorf("field %s reference table: %w", field.Name, err)
	}
	return nil
}
