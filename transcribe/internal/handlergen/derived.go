package handlergen

import gen "github.com/viant/datly/transcribe/generate"

// PrepareDerived validates the derived body/output assignment before preparing artifacts.
func (g *Generation) PrepareDerived() error {
	contractPlan, err := gen.New(*g.input).Plan()
	if err != nil {
		return err
	}
	semantic, err := g.compilePlan()
	if err != nil {
		return err
	}
	if err = g.validateContractAssignment(semantic, contractPlan); err != nil {
		return err
	}
	return g.prepare(semantic)
}
