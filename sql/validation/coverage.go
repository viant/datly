package validation

import (
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlx/io"
	xhandler "github.com/viant/xdatly/handler"
)

func (p *plan) checkCoverage(policy xhandler.ValidationOptions) error {
	if policy.Action == xhandler.WriteInsert {
		return nil
	}
	byColumn := make(map[string]string, len(p.columns))
	keys := 0
	for index, column := range p.columns {
		fields, ok := column.(io.Fielder)
		if !ok {
			return fmt.Errorf("validation column %s has no canonical Go field", column.Name())
		}
		path := fields.Fields()
		name := path[len(path)-1].Name
		byColumn[column.Name()] = name
		if tag := column.Tag(); tag != nil && tag.PrimaryKey {
			keys++
			if !policy.PreviousFields.Has(name) {
				return fmt.Errorf("previous identity field %s was not loaded", name)
			}
			parameter := make([]any, 1)
			p.bind(policy.Previous, parameter, index, 1)
			value, err := driver.DefaultParameterConverter.ConvertValue(parameter[0])
			if err != nil {
				return fmt.Errorf("previous identity field %s: %w", name, err)
			}
			if value == nil {
				return fmt.Errorf("previous identity field %s is null", name)
			}
			if bytes, ok := value.([]byte); ok && bytes == nil {
				return fmt.Errorf("previous identity field %s is null", name)
			}
		}
	}
	if keys == 0 {
		return fmt.Errorf("update validation requires declared primary keys")
	}
	for _, column := range p.columns {
		tag := column.Tag()
		if tag == nil || tag.UniqueDep == "" {
			continue
		}
		name, dependency := byColumn[column.Name()], byColumn[tag.UniqueDep]
		if dependency == "" {
			return fmt.Errorf("unique dependency column %s is not mapped", tag.UniqueDep)
		}
		if !policy.Fields.Has(name) && !policy.Fields.Has(dependency) {
			continue
		}
		for _, field := range []string{name, dependency} {
			if !policy.Fields.Has(field) && !policy.PreviousFields.Has(field) {
				return fmt.Errorf("previous unique dependency field %s was not loaded", field)
			}
		}
	}
	return nil
}
