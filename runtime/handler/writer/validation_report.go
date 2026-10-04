package writer

import (
	"context"
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

// inputValidationReport keeps native validation authoritative while allowing
// the once-per-input business callback to append its own ordered violations.
type inputValidationReport struct {
	native   *xhandler.Validation
	business []*xhandler.Violation
}

var _ xhandler.ValidationReport = (*inputValidationReport)(nil)

func (r *inputValidationReport) SchemaFailed() bool { return r.native.Err() != nil }
func (r *inputValidationReport) SchemaViolations() []xhandler.ValidationIssue {
	if r.native == nil {
		return nil
	}
	result := make([]xhandler.ValidationIssue, 0, len(r.native.Violations))
	for _, v := range r.native.Violations {
		if v != nil {
			result = append(result, xhandler.ValidationIssue{Location: v.Location, Field: v.Field, Message: v.Message, Check: v.Check})
		}
	}
	return result
}
func (r *inputValidationReport) Add(v xhandler.Violation) { r.business = append(r.business, &v) }
func (r *inputValidationReport) result() *xhandler.Validation {
	result := &xhandler.Validation{}
	if r.native != nil {
		result.Failed = r.native.Failed
		result.Code = r.native.Code
		result.Violations = append(result.Violations, r.native.Violations...)
	}
	result.Violations = append(result.Violations, r.business...)
	return result
}

func validateAggregateHooks(root *Record, inputType, outputType reflect.Type) error {
	if root != nil && root.HookType != nil {
		method := reflect.New(root.HookType).MethodByName("ObservePhase")
		if method.IsValid() {
			typ := method.Type()
			if root.Auxiliary || typ.IsVariadic() || typ.NumIn() != 2 || typ.NumOut() != 0 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.TypeFor[xhandler.PhaseEvent]() {
				return fmt.Errorf("ObservePhase requires physical root and canonical PhaseEvent signature")
			}
		}
	}
	aggregate := false
	if root != nil && root.HookType != nil {
		method := reflect.New(root.HookType).MethodByName("ValidateInput")
		if method.IsValid() {
			typ := method.Type()
			if root.Auxiliary || typ.IsVariadic() || typ.NumIn() != 4 || typ.NumOut() != 1 || typ.Out(0) != reflect.TypeFor[error]() || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.In(2) != reflect.PointerTo(outputType) || typ.In(3) != reflect.TypeFor[xhandler.ValidationReport]() {
				return fmt.Errorf("ValidateInput requires physical root and canonical Input, Output, ValidationReport")
			}
			aggregate = true
		}
	}
	var visit func(*Record) error
	visit = func(record *Record) error {
		if record == nil {
			return nil
		}
		if record.HookType != nil {
			hook := reflect.New(record.HookType)
			if record != root && hook.MethodByName("ObservePhase").IsValid() {
				return fmt.Errorf("ObservePhase is only supported on the writer root")
			}
			if record != root && hook.MethodByName("ValidateInput").IsValid() {
				return fmt.Errorf("ValidateInput is only supported on the writer root")
			}
			if aggregate && hook.MethodByName("Validate").IsValid() {
				return fmt.Errorf("aggregate ValidateInput cannot overlap row Validate at %s", record.Path)
			}
		}
		for _, relation := range record.Relations {
			if err := visit(relation.Child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

// Only ordinary results take this path. Validator-returned errors remain fatal,
// even if an operational error happens to wrap a Validation value.
type initialSchemaViolations struct{ validation *xhandler.Validation }

func (e *initialSchemaViolations) Error() string { return e.validation.Error() }
func (e *initialSchemaViolations) Unwrap() error { return e.validation }
