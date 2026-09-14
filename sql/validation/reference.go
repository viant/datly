package validation

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/viant/sqlx/io"
	sqlvalidator "github.com/viant/sqlx/io/validator"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

// MatchReference resolves an exact native constraint and compares SQL-bound
// values in the active default transaction. The generated caller separately
// proves parent INSERT policy, captured edge authority and execution order.
// This method neither queries the database nor issues a receipt for that proof.
func (s *Service) MatchReference(ctx context.Context, child, parent any, expected xhandler.ValidationReference) (*xhandler.ValidationReference, error) {
	if ctx == nil {
		return nil, fmt.Errorf("reference context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || (xshape.Runtime{}).IsNil(s.source) {
		return nil, fmt.Errorf("reference validation source is required")
	}
	if expected.Field == "" || expected.Table == "" || expected.Column == "" {
		return nil, fmt.Errorf("reference field, table and column are required")
	}
	connection, err := s.source.ValidationConnection(ctx, "")
	if err != nil {
		return nil, err
	}
	if connection.Tx == nil {
		return nil, fmt.Errorf("reference matching requires the active mutation transaction")
	}
	childPlan, err := s.referencePlan(child)
	if err != nil {
		return nil, err
	}
	var matched *xhandler.ValidationReference
	for _, check := range childPlan.nativeChecks.RefKey {
		ref := check.Reference
		targetMatches, err := ref.MatchesTarget(sqlvalidator.Reference{Field: expected.Field, Schema: expected.Schema, Table: expected.Table, Column: expected.Column}, connection.Dialect)
		if err != nil {
			return nil, fmt.Errorf("reference target identity: %w", err)
		}
		if ref.Field != expected.Field || !targetMatches || ref.Column != expected.Column {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("ambiguous native reference for field %s", expected.Field)
		}
		matched = &xhandler.ValidationReference{Field: ref.Field, Schema: ref.Schema, Table: ref.Table, Column: ref.Column}
	}
	if matched == nil {
		return nil, nil
	}
	parentPlan, err := s.referencePlan(parent)
	if err != nil {
		return nil, err
	}
	childValue, err := childPlan.referenceValue(child, expected.Field, true)
	if err != nil {
		return nil, err
	}
	parentValue, err := parentPlan.referenceValue(parent, expected.Column, false)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(childValue, parentValue) {
		return nil, fmt.Errorf("reference field %s does not match the pending parent value", expected.Field)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return matched, nil
}

func (s *Service) referencePlan(value any) (*plan, error) {
	if _, err := s.options(value, []any{xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}}); err != nil {
		return nil, err
	}
	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return s.plan(typ)
}

func (p *plan) referenceValue(entity any, name string, fieldName bool) (driver.Value, error) {
	position := -1
	for index, column := range p.columns {
		candidate := column.Name()
		if fieldName {
			fields := column.(io.Fielder).Fields()
			candidate = fields[len(fields)-1].Name
		}
		if candidate != name {
			continue
		}
		if position != -1 {
			return nil, fmt.Errorf("ambiguous reference binding %s", name)
		}
		position = index
	}
	if position < 0 {
		return nil, fmt.Errorf("reference binding %s is not mapped", name)
	}
	parameters := make([]any, 1)
	p.bind(entity, parameters, position, 1)
	value, err := driver.DefaultParameterConverter.ConvertValue(parameters[0])
	if err != nil {
		return nil, fmt.Errorf("reference binding %s: %w", name, err)
	}
	if value == nil {
		return nil, fmt.Errorf("reference binding %s is null", name)
	}
	if bytes, ok := value.([]byte); ok && bytes == nil {
		return nil, fmt.Errorf("reference binding %s is null", name)
	}
	return value, nil
}
