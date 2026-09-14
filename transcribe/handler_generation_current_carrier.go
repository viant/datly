package transcribe

import (
	"fmt"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

// currentRecordTypes separates the bound carrier from the canonical read row.
// An envelope's root holder is declared by the actual read projection at runtime;
// neither a field named Data nor the carrier's first field establishes authority.
func (g *handlerGeneration) currentRecordTypes(generated *gen.Plan, current *plan.CurrentPlan) (rows, carrier string, err error) {
	if current == nil || generated == nil {
		return "", "", fmt.Errorf("current record type requires view and contract plans")
	}
	bound, err := g.inputFieldType(generated, current.InputPath)
	if err != nil {
		return "", "", err
	}
	if _, err := (xshape.Resolver{}).Reference(bound); err != nil {
		return "", "", fmt.Errorf("current bound carrier: %w", err)
	}
	var owner *gen.ViewPlan
	for index := range generated.Views {
		view := &generated.Views[index]
		if view.Identity != current.ViewIdentity {
			continue
		}
		if owner != nil {
			return "", "", fmt.Errorf("current view %s has multiple final type authorities", current.ViewIdentity)
		}
		owner = view
	}
	if owner == nil || owner.Type == "" {
		return "", "", fmt.Errorf("current view %s has no final row type authority", current.ViewIdentity)
	}
	root, err := (xshape.Resolver{}).Reference(owner.Type)
	if err != nil {
		return "", "", fmt.Errorf("current view %s row type: %w", current.ViewIdentity, err)
	}
	if len(root.Wrappers) != 0 {
		return "", "", fmt.Errorf("current view %s row type must be an unwrapped named type", current.ViewIdentity)
	}
	base, directErr := g.recordBase(bound, spec.CardinalityMany)
	if directErr == nil {
		canonicalBase, err := generated.CanonicalType(g.input.TargetPackage, base)
		if err != nil {
			return "", "", err
		}
		canonicalOwner, err := generated.CanonicalType(g.input.TargetPackage, owner.Type)
		if err != nil {
			return "", "", err
		}
		if canonicalBase != canonicalOwner {
			return "", "", fmt.Errorf("current view %s bound rows %s differ from canonical row %s", current.ViewIdentity, canonicalBase, canonicalOwner)
		}
		return bound, "", nil
	}
	// Named collections and wrapped results retain their actual carrier type.
	// Generated capture unwraps the declared root holder and normalizes exact
	// typed rows through native shape.Collection before freezing Previous.
	return "[]*" + owner.Type, bound, nil
}
