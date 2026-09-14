package validation

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/govalidator"
	"github.com/viant/sqlx/io"
	sqlvalidator "github.com/viant/sqlx/io/validator"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

// validationBatch keeps caller-matched policy and diagnostic locations aligned
// while SQLX owns constraint evaluation over the entire typed candidate set.
type validationBatch struct {
	values         any
	at             io.ValueAccessor
	policies       []xhandler.ValidationOptions
	compiled       *plan
	locations      map[string]string
	nativePolicies []sqlvalidator.CandidatePolicy
	goPlans        []*govalidator.Prepared
	references     bool
}

func (s *Service) batch(value any, policies []xhandler.ValidationOptions) (*validationBatch, error) {
	typ := reflect.TypeOf(value)
	if typ == nil || typ.Kind() != reflect.Slice {
		return nil, fmt.Errorf("framework batch validation requires a typed entity slice, got %T", value)
	}
	typ = typ.Elem()
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("framework batch validation requires a typed entity slice, got %T", value)
	}
	at, count, err := io.Values(value)
	if err != nil {
		return nil, err
	}
	if count != len(policies) {
		return nil, fmt.Errorf("validation policy count %d does not match candidate count %d", len(policies), count)
	}
	batch := &validationBatch{values: value, at: at, policies: make([]xhandler.ValidationOptions, count), locations: make(map[string]string)}
	// Validate every option before native plan construction or connection access.
	for i, policy := range policies {
		if batch.policies[i], err = s.options(at(i), []any{policy}); err != nil {
			return nil, fmt.Errorf("validation candidate %d: %w", i, err)
		}
		if i > 0 && policy.Connector != policies[0].Connector {
			return nil, fmt.Errorf("validation batch candidates must use the same connector")
		}
		if batch.policies[i].Location == "" {
			batch.policies[i].Location = fmt.Sprintf("[%d]", i)
		}
	}
	if count == 0 {
		return batch, nil
	}
	if batch.compiled, err = s.plan(typ); err != nil {
		return nil, err
	}
	batch.nativePolicies = make([]sqlvalidator.CandidatePolicy, count)
	batch.goPlans = make([]*govalidator.Prepared, count)
	for i, policy := range batch.policies {
		prepared, prepareErr := batch.compiled.prepare(policy)
		if prepareErr != nil {
			return nil, fmt.Errorf("validation candidate %d: %w", i, prepareErr)
		}
		batch.policies[i], batch.nativePolicies[i], batch.goPlans[i] = prepared.policy, prepared.native, prepared.goPlan
		batch.references = batch.references || len(policy.SatisfiedReferences) != 0
	}
	for i, policy := range batch.policies {
		if err = batch.compiled.checkCoverage(policy); err != nil {
			return nil, fmt.Errorf("validation candidate %d: %w", i, err)
		}
		for _, column := range batch.compiled.columns {
			fields := column.(io.Fielder).Fields()
			field := fields[len(fields)-1].Name
			batch.locations[fmt.Sprintf("_batch[%d].%s", i, field)] = policy.Location + "." + field
		}
	}
	return batch, nil
}

func (s *Service) validateBatch(ctx context.Context, value any, policies []xhandler.ValidationOptions) (*xhandler.Validation, error) {
	batch, err := s.batch(value, policies)
	if err != nil {
		return nil, err
	}
	result := &xhandler.Validation{}
	if len(batch.policies) == 0 {
		return result, nil
	}
	if (xshape.Runtime{}).IsNil(s.source) {
		return nil, fmt.Errorf("framework validation source is required")
	}
	connection, err := s.source.ValidationConnection(ctx, batch.policies[0].Connector)
	if err != nil {
		return nil, err
	}
	if batch.references && connection.Tx == nil {
		return nil, fmt.Errorf("satisfied references require the active mutation transaction")
	}
	for i := range batch.policies {
		goResult, err := s.validateGoEntity(ctx, batch.at(i), batch.goPlans[i])
		if err != nil {
			return nil, err
		}
		result.Violations = append(result.Violations, goResult.Violations...)
	}
	nativeOptions := []sqlvalidator.Option{sqlvalidator.WithShallow(true), sqlvalidator.WithLocation("_batch"), sqlvalidator.WithCandidatePolicies(batch.nativePolicies)}
	if connection.Tx != nil {
		nativeOptions = append(nativeOptions, sqlvalidator.WithTransaction(connection.Tx))
	}
	sqlResult, err := s.sqlValidator.Validate(ctx, connection.DB, batch.values, nativeOptions...)
	if err != nil {
		return nil, fmt.Errorf("SQL validation: %w", err)
	}
	for _, violation := range sqlResult.Violations {
		location, ok := batch.locations[violation.Location]
		if !ok {
			return nil, fmt.Errorf("native validation returned an unrecognized candidate location")
		}
		violation.Location = location
	}
	batch.compiled.appendSQL(result, sqlResult)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	result.Failed = len(result.Violations) > 0
	return result, nil
}
