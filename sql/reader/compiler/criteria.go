package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/sql/criteria"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
)

// compileCriteria attaches type-aware immutable criteria policy to prepared
// views. Actual Go row fields are authoritative; SQL data type labels are not
// treated as Go type expressions at request time.
func (b *planCompiler) compileCriteria(views *viewSet) (map[*data.View]*criteria.Compiler, error) {
	result := make(map[*data.View]*criteria.Compiler, len(views.rowTypes))
	for view, rowType := range views.rowTypes {
		compiled := &criteria.Compiler{Columns: map[string]criteria.Column{}, Methods: map[string]criteria.Method{}}
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			field, found := typecatalog.FieldByName(rowType, column.Name)
			if !found {
				return nil, fmt.Errorf("criteria column %s.%s has no typed row field", view.Spec.Name, column.Name)
			}
			formatting, err := format.Parse(field.Tag)
			if err != nil {
				return nil, fmt.Errorf("criteria column %s.%s format: %w", view.Spec.Name, column.Name, err)
			}
			expression := column.Expression
			if expression == "" {
				expression = column.Column
			}
			if expression == "" {
				expression = column.Name
			}
			definition := criteria.Column{Expression: expression, Type: field.Type}
			if formatting != nil {
				definition.TimeLayout = formatting.TimeLayout
			}
			compiled.Columns[column.Name] = definition
			if column.Column != "" {
				compiled.Columns[column.Column] = definition
			}
		}
		if view.Spec.Selector != nil {
			for _, method := range view.Spec.Selector.SQLMethods {
				name := strings.TrimSpace(method.Name)
				key := strings.ToLower(name)
				if name == "" {
					return nil, fmt.Errorf("view %s criteria method name is required", view.Spec.Name)
				}
				if _, exists := compiled.Methods[key]; exists {
					return nil, fmt.Errorf("view %s criteria method %q is duplicated", view.Spec.Name, name)
				}
				fields := make([]xshape.RuntimeField, len(method.Args))
				for i, expression := range method.Args {
					fields[i] = xshape.RuntimeField{Name: fmt.Sprintf("Arg%d", i), TypeExpr: expression}
				}
				argumentType, err := (xshape.Runtime{Lookup: b.input.TypeLookup}).Struct(fields)
				if err != nil {
					return nil, fmt.Errorf("view %s criteria method %s: %w", view.Spec.Name, name, err)
				}
				definition := criteria.Method{Name: name}
				for i := range fields {
					definition.Args = append(definition.Args, argumentType.Field(i).Type)
				}
				if err := definition.Validate(); err != nil {
					return nil, fmt.Errorf("view %s criteria method: %w", view.Spec.Name, err)
				}
				compiled.Methods[key] = definition
			}
		}
		result[view] = compiled
	}
	return result, nil
}
