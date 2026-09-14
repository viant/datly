package validation

import (
	sqlvalidator "github.com/viant/sqlx/io/validator"
	xhandler "github.com/viant/xdatly/handler"
)

// candidate converts framework facts to native policy. Both scalar and batch
// calls use this path before executing any Go or SQL constraint checks.
func (p *plan) candidate(policy xhandler.ValidationOptions) (sqlvalidator.CandidatePolicy, error) {
	result := sqlvalidator.CandidatePolicy{Previous: policy.Previous}
	if policy.Fields != nil {
		result.FieldFilter = policy.Fields.Has
	}
	if policy.DeferredFields != nil {
		result.DeferredFields = policy.DeferredFields.Has
	}
	for _, ref := range policy.SatisfiedReferences {
		result.SatisfiedReferences = append(result.SatisfiedReferences, sqlvalidator.Reference{Field: ref.Field, Schema: ref.Schema, Table: ref.Table, Column: ref.Column})
	}
	if err := p.nativeChecks.ValidateReferences(result.SatisfiedReferences); err != nil {
		return sqlvalidator.CandidatePolicy{}, err
	}
	return result, nil
}
