package spec

import (
	"fmt"
	"strings"
)

// IsMutationInput identifies generated body or internal writer root state.
func (p *Parameter) IsMutationInput() bool {
	if p == nil || p.EmitOutput {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(p.Source.Kind))
	return kind == "body" || kind == "internal"
}

// ValidateInternalMutationRoot limits nontransport roots to explicit DELETE graphs.
func ValidateInternalMutationRoot(component *Component, operation string) error {
	if component == nil {
		return fmt.Errorf("mutation component is required")
	}
	var internal *Parameter
	roots := 0
	for _, p := range EffectiveParameters(component.Parameters) {
		if !p.IsMutationInput() {
			continue
		}
		roots++
		if strings.EqualFold(strings.TrimSpace(p.Source.Kind), "internal") {
			internal = p
		}
	}
	if internal == nil {
		return nil
	}
	if roots != 1 {
		return fmt.Errorf("internal mutation root is ambiguous with another mutation input")
	}
	if operation != "put" && operation != "patch" {
		return fmt.Errorf("internal mutation root requires PUT or PATCH operation")
	}
	if len(component.Routes) == 0 {
		return fmt.Errorf("internal mutation root requires an explicit DELETE route")
	}
	for _, route := range component.Routes {
		if route == nil || !strings.EqualFold(strings.TrimSpace(route.Method), "DELETE") {
			return fmt.Errorf("internal mutation root requires exclusively DELETE routes")
		}
	}
	if strings.TrimSpace(internal.Source.Name) != "" {
		return fmt.Errorf("internal mutation root cannot name a transport source")
	}
	if internal.Required != nil && *internal.Required {
		return fmt.Errorf("internal mutation root cannot require client input")
	}
	return nil
}
