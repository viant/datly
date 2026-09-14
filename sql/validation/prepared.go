package validation

import (
	"github.com/viant/govalidator"
	sqlvalidator "github.com/viant/sqlx/io/validator"
	xhandler "github.com/viant/xdatly/handler"
)

// preparedCandidate retains one policy and the exact native Go preparation
// approved before connection access or execution of any candidate's predicates.
type preparedCandidate struct {
	policy xhandler.ValidationOptions
	goPlan *govalidator.Prepared
	native sqlvalidator.CandidatePolicy
}

type fieldSnapshot map[string]bool

func (s fieldSnapshot) Has(name string) bool { return s[name] }

func (p *plan) snapshot(fields xhandler.FieldSet) xhandler.FieldSet {
	if fields == nil {
		return nil
	}
	result := make(fieldSnapshot, len(p.fields))
	for _, name := range p.fields {
		result[name] = fields.Has(name)
	}
	return result
}

func (p *plan) prepare(policy xhandler.ValidationOptions) (preparedCandidate, error) {
	policy.Fields = p.snapshot(policy.Fields)
	policy.DeferredFields = p.snapshot(policy.DeferredFields)
	policy.PreviousFields = p.snapshot(policy.PreviousFields)
	result := preparedCandidate{policy: policy}
	var err error
	if result.native, err = p.candidate(policy); err != nil {
		return preparedCandidate{}, err
	}
	options := []govalidator.Option{govalidator.WithShallow(true)}
	if policy.Location != "" {
		options = append(options, govalidator.WithPath(govalidator.NewPath().Field(policy.Location)))
	}
	if policy.Fields != nil {
		options = append(options, govalidator.WithFieldFilter(policy.Fields.Has))
	}
	if policy.DeferredFields != nil {
		options = append(options, govalidator.WithDeferredFields(policy.DeferredFields.Has))
	}
	if result.goPlan, err = p.goChecks.Prepare(options...); err != nil {
		return preparedCandidate{}, err
	}
	return result, nil
}
