// Package validation combines native Go and SQL constraint checks for one
// invocation-scoped typed entity. It owns validation policy, not transactions.
package validation

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"sync"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/govalidator"
	"github.com/viant/sqlx/io"
	sqlvalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/sqlx/metadata/info"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

// Connection is an internal implementation carrier. Public handler capabilities
// receive Validator only, never this database/transaction pair.
type Connection struct {
	DB *sql.DB
	Tx *sql.Tx
	// Dialect is existing source authority, not inferred from identifier syntax.
	Dialect *info.Dialect
}
type Source interface {
	ValidationConnection(context.Context, string) (Connection, error)
}

type Service struct {
	source       Source
	sqlValidator *sqlvalidator.Service
	plans        sync.Map
}
type plan struct {
	columns      []io.Column
	bind         io.PlaceholderBinder
	nativeChecks *sqlvalidator.Checks
	goChecks     *govalidator.Checks
	fields       []string
}

func New(source Source) *Service {
	return &Service{source: source, sqlValidator: sqlvalidator.New()}
}

func (s *Service) Validate(ctx context.Context, value any, options ...any) (*xhandler.Validation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("framework validation context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(options) == 1 {
		if policies, ok := options[0].([]xhandler.ValidationOptions); ok {
			return s.validateBatch(ctx, value, policies)
		}
	}
	policy, err := s.options(value, options)
	if err != nil {
		return nil, err
	}
	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	compiled, err := s.plan(typ)
	if err != nil {
		return nil, err
	}
	candidate, err := compiled.prepare(policy)
	if err != nil {
		return nil, err
	}
	policy = candidate.policy
	if err = compiled.checkCoverage(policy); err != nil {
		return nil, err
	}
	if (xshape.Runtime{}).IsNil(s.source) {
		return nil, fmt.Errorf("framework validation source is required")
	}
	connection, err := s.source.ValidationConnection(ctx, policy.Connector)
	if err != nil {
		return nil, err
	}
	if len(policy.SatisfiedReferences) != 0 && connection.Tx == nil {
		return nil, fmt.Errorf("satisfied references require the active mutation transaction")
	}
	result, err := s.validateGoEntity(ctx, value, candidate.goPlan)
	if err != nil {
		return nil, err
	}
	native := []sqlvalidator.Option{sqlvalidator.WithShallow(true), sqlvalidator.WithCandidatePolicies([]sqlvalidator.CandidatePolicy{candidate.native}), sqlvalidator.WithLocation(policy.Location)}
	if connection.Tx != nil {
		native = append(native, sqlvalidator.WithTransaction(connection.Tx))
	}
	sqlResult, err := s.sqlValidator.Validate(ctx, connection.DB, value, native...)
	if err != nil {
		return nil, fmt.Errorf("SQL validation: %w", err)
	}
	compiled.appendSQL(result, sqlResult)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	result.Failed = len(result.Violations) > 0
	return result, nil
}

func (s *Service) validateGoEntity(ctx context.Context, value any, prepared *govalidator.Prepared) (*xhandler.Validation, error) {
	result := &xhandler.Validation{}
	goResult, err := s.validateGo(ctx, value, prepared)
	if err != nil {
		return nil, fmt.Errorf("Go validation: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for _, violation := range goResult.Violations {
		result.Violations = append(result.Violations, &xhandler.Violation{Location: violation.Location, Field: violation.Field, Message: violation.Message, Check: violation.Check})
	}
	return result, nil
}

func (p *plan) appendSQL(result *xhandler.Validation, sqlResult *sqlvalidator.Validation) {
	for _, violation := range sqlResult.Violations {
		// Native SQLX messages may include raw values. The public result reports
		// check/field without automatically disclosing database or input values.
		message := "field " + violation.Field + " failed " + violation.Check + " validation"
		for _, column := range p.columns {
			fields := column.(io.Fielder).Fields()
			if fields[len(fields)-1].Name == violation.Field && column.Tag() != nil && column.Tag().ErrorMgs != "" {
				message = violation.Message
				break
			}
		}
		result.Violations = append(result.Violations, &xhandler.Violation{Location: violation.Location, Field: violation.Field, Message: message, Check: violation.Check})
	}
}

func (s *Service) plan(typ reflect.Type) (*plan, error) {
	if existing, ok := s.plans.Load(typ); ok {
		return existing.(*plan), nil
	}
	columns, bind, err := io.StructColumnMapper(typ)
	if err != nil {
		return nil, err
	}
	goChecks, err := govalidator.NewChecks(typ)
	if err != nil {
		return nil, fmt.Errorf("Go validation plan: %w", err)
	}
	nativeChecks, err := sqlvalidator.NewChecks(typ, nil)
	if err != nil {
		return nil, fmt.Errorf("SQL validation plan: %w", err)
	}
	compiled := &plan{columns: columns, bind: bind, nativeChecks: nativeChecks, goChecks: goChecks}
	names := map[string]bool{}
	for _, name := range goChecks.InputFields() {
		names[name] = true
	}
	for _, column := range columns {
		fields := column.(io.Fielder).Fields()
		names[fields[len(fields)-1].Name] = true
	}
	for name := range names {
		compiled.fields = append(compiled.fields, name)
	}
	sort.Strings(compiled.fields)
	actual, _ := s.plans.LoadOrStore(typ, compiled)
	return actual.(*plan), nil
}

func (s *Service) validateGo(ctx context.Context, value any, prepared *govalidator.Prepared) (result *govalidator.Validation, err error) {
	// Unsupported native reflection shapes must fail the phase, never crash an
	// invocation or masquerade as a successful validation. Do not expose panic
	// values, which can originate in application-registered predicates.
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = dexec.NewPanicError("native Go validation", recovered)
		}
	}()
	return prepared.Validate(ctx, value)
}
