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

// Names are immutable schema authority; bits are captured anew for each phase.
type fieldSnapshot struct {
	indexes  map[string]int
	bits     uint64
	overflow []uint64
}

func (s fieldSnapshot) Has(name string) bool {
	index, ok := s.indexes[name]
	if !ok {
		return false
	}
	if index < 64 {
		return s.bits&(uint64(1)<<uint(index)) != 0
	}
	index -= 64
	return s.overflow[index/64]&(uint64(1)<<uint(index%64)) != 0
}

func (p *plan) snapshot(fields xhandler.FieldSet) xhandler.FieldSet {
	if fields == nil {
		return nil
	}
	result := fieldSnapshot{indexes: p.fieldIndexes}
	if len(p.fields) > 64 {
		result.overflow = make([]uint64, (len(p.fields)-64+63)/64)
	}
	for index, name := range p.fields {
		if !fields.Has(name) {
			continue
		}
		if index < 64 {
			result.bits |= uint64(1) << uint(index)
		} else {
			result.overflow[(index-64)/64] |= uint64(1) << uint((index-64)%64)
		}
	}
	return result
}

func (p *plan) prepare(policy xhandler.ValidationOptions) (preparedCandidate, error) {
	policy.Fields = p.snapshot(policy.Fields)
	if policy.CheckOnly {
		if policy.GoFields == nil {
			policy.GoFields = policy.Fields
		} else {
			policy.GoFields = p.snapshot(policy.GoFields)
		}
	}
	policy.DeferredFields = p.snapshot(policy.DeferredFields)
	policy.PreviousFields = p.snapshot(policy.PreviousFields)
	result := preparedCandidate{policy: policy}
	var err error
	if policy.CheckOnly {
		result.native = sqlvalidator.CandidatePolicy{FieldFilter: policy.Fields.Has}
	} else if result.native, err = p.candidate(policy); err != nil {
		return preparedCandidate{}, err
	}
	options := []govalidator.Option{govalidator.WithShallow(true)}
	if policy.Location != "" {
		options = append(options, govalidator.WithPath(govalidator.NewPath().Field(policy.Location)))
	}
	goFields := policy.Fields
	if policy.CheckOnly {
		goFields = policy.GoFields
	}
	if goFields != nil {
		options = append(options, govalidator.WithFieldFilter(goFields.Has))
	}
	if policy.DeferredFields != nil {
		options = append(options, govalidator.WithDeferredFields(policy.DeferredFields.Has))
	}
	if result.goPlan, err = p.goChecks.Prepare(options...); err != nil {
		return preparedCandidate{}, err
	}
	return result, nil
}
