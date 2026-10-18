package validation

import (
	"context"
	"fmt"
	"reflect"

	sqlvalidator "github.com/viant/sqlx/io/validator"
	xhandler "github.com/viant/xdatly/handler"
)

// BufferedParent is internal owner evidence, never a request-bindable receipt.
// The DML owner supplies only earlier typed inserts and preserves their order.
type BufferedParent struct {
	Table string
	Value any
	Check func(context.Context, xhandler.ValidationReference) error
}

// PrepareBufferedValidation reuses the native constraint plan and receipt
// mechanism for references produced by an earlier component in the same buffer.
// It does not execute DML or start a transaction.
func (s *Service) PrepareBufferedValidation(ctx context.Context, value any, options []any, parents []BufferedParent) ([]any, []func(context.Context) error, error) {
	var values []any
	var policies []xhandler.ValidationOptions
	batch := false
	if len(options) == 1 {
		if requested, ok := options[0].([]xhandler.ValidationOptions); ok {
			prepared, err := s.batch(ctx, value, requested)
			if err != nil {
				return nil, nil, err
			}
			batch = true
			policies = prepared.policies
			for i := range policies {
				values = append(values, prepared.at(i))
			}
		}
	}
	if !batch {
		policy, err := s.options(value, options)
		if err != nil {
			return nil, nil, err
		}
		plan, err := s.referencePlan(ctx, value)
		if err != nil {
			return nil, nil, err
		}
		prepared, err := plan.prepare(policy)
		if err != nil {
			return nil, nil, err
		}
		policy = prepared.policy
		if err = plan.checkCoverage(policy); err != nil {
			return nil, nil, err
		}
		values, policies = []any{value}, []xhandler.ValidationOptions{policy}
	}
	var guards []func(context.Context) error
	for i, policy := range policies {
		if policy.Action != xhandler.WriteInsert || policy.CheckOnly || policy.Connector != "" || policy.DeferredFields != nil || !checkEnabled(policy.CheckRef) {
			continue
		}
		connection, err := s.source.ValidationConnection(ctx, "")
		if err != nil {
			return nil, nil, err
		}
		if connection.Tx == nil {
			continue
		}
		child := values[i]
		plan, err := s.referencePlan(ctx, child)
		if err != nil {
			return nil, nil, err
		}
		for _, check := range plan.nativeChecks.RefKey {
			ref := check.Reference
			if policy.Fields != nil && !policy.Fields.Has(ref.Field) {
				continue
			}
			expected := xhandler.ValidationReference{Field: ref.Field, Schema: ref.Schema, Table: ref.Table, Column: ref.Column}
			found := false
			for _, receipt := range policies[i].SatisfiedReferences {
				if receipt == expected {
					found = true
					break
				}
			}
			if found {
				continue
			}
			for _, parent := range parents {
				if parent.Check == nil {
					continue
				}
				matches, err := ref.MatchesTarget(sqlvalidator.Reference{Field: ref.Field, Table: parent.Table, Column: ref.Column}, connection.Dialect)
				if err != nil || !matches {
					continue
				}
				if err := parent.Check(ctx, expected); err != nil {
					continue
				}
				matched, err := s.MatchReference(ctx, child, parent.Value, expected)
				if err != nil || matched == nil {
					continue
				}
				// Capture a detached SQL-bound key: comparing two mutable rows
				// later would miss a coordinated change to both reference values.
				key, err := plan.referenceValue(child, ref.Field, true)
				if err != nil {
					continue
				}
				if bytes, ok := key.([]byte); ok {
					key = append([]byte(nil), bytes...)
				}
				parentPlan, err := s.referencePlan(ctx, parent.Value)
				if err != nil {
					continue
				}
				parent := parent
				guards = append(guards, func(checkCtx context.Context) error {
					if err := parent.Check(checkCtx, expected); err != nil {
						return err
					}
					actual, err := plan.referenceValue(child, expected.Field, true)
					if err != nil {
						return err
					}
					if !reflect.DeepEqual(actual, key) {
						return fmt.Errorf("buffered reference %s changed", expected.Field)
					}
					parentKey, err := parentPlan.referenceValue(parent.Value, expected.Column, false)
					if err != nil {
						return err
					}
					if !reflect.DeepEqual(parentKey, key) {
						return fmt.Errorf("buffered reference %s no longer matches", expected.Field)
					}
					return nil
				})
				policies[i].SatisfiedReferences = append(policies[i].SatisfiedReferences, expected)
				break
			}
		}
	}
	if batch {
		return []any{policies}, guards, nil
	}
	return []any{policies[0]}, guards, nil
}
