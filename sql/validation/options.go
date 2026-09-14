package validation

import (
	"fmt"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

func (s *Service) options(value any, options []any) (xhandler.ValidationOptions, error) {
	var policy xhandler.ValidationOptions
	if len(options) != 1 {
		return policy, fmt.Errorf("framework validation requires exactly one ValidationOptions value")
	}
	var ok bool
	policy, ok = options[0].(xhandler.ValidationOptions)
	if !ok {
		return policy, fmt.Errorf("unsupported framework validation options %T", options[0])
	}
	shape := xshape.Runtime{}
	if shape.IsNil(policy.DeferredFields) {
		policy.DeferredFields = nil
	}
	if policy.DeferredFields != nil && len(policy.SatisfiedReferences) != 0 {
		return policy, fmt.Errorf("deferred fields cannot accompany satisfied references")
	}
	policy.SatisfiedReferences = append([]xhandler.ValidationReference(nil), policy.SatisfiedReferences...)
	if shape.IsNil(value) {
		return policy, fmt.Errorf("framework validation entity is required")
	}
	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return policy, fmt.Errorf("framework validation requires one typed entity, got %T", value)
	}
	if !policy.Shallow {
		return policy, fmt.Errorf("framework validation requires shallow per-entity calls")
	}
	switch policy.Action {
	case xhandler.WriteInsert:
		if !shape.IsNil(policy.Previous) || !shape.IsNil(policy.PreviousFields) || !shape.IsNil(policy.Fields) {
			return policy, fmt.Errorf("insert validation requires no previous row and full coverage")
		}
		policy.Previous, policy.PreviousFields, policy.Fields = nil, nil, nil
	case xhandler.WriteUpdate:
		if policy.DeferredFields != nil {
			return policy, fmt.Errorf("deferred fields require an insert validation policy")
		}
		if len(policy.SatisfiedReferences) != 0 {
			return policy, fmt.Errorf("satisfied references require an insert validation policy")
		}
		if shape.IsNil(policy.Previous) || shape.IsNil(policy.PreviousFields) || shape.IsNil(policy.Fields) {
			return policy, fmt.Errorf("update validation requires previous row, read provenance and explicit coverage")
		}
		previousType := reflect.TypeOf(policy.Previous)
		if previousType.Kind() == reflect.Ptr {
			previousType = previousType.Elem()
		}
		if previousType != typ {
			return policy, fmt.Errorf("previous entity type %v does not match %v", previousType, typ)
		}
	default:
		return policy, fmt.Errorf("unsupported framework validation action %q", policy.Action)
	}
	return policy, nil
}
