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

// ValidateInternalMutationRoot keeps server-prepared mutation roots separate
// from client binding while supporting the same operation/transport combinations
// as generated writers.
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
	if operation != "post" && operation != "put" && operation != "patch" {
		return fmt.Errorf("internal mutation root requires POST, PUT or PATCH operation")
	}
	if len(component.Routes) == 0 {
		return fmt.Errorf("internal mutation root requires an explicit write route")
	}
	for _, route := range component.Routes {
		if route == nil {
			return fmt.Errorf("internal mutation root requires an explicit write route")
		}
		method := strings.ToUpper(strings.TrimSpace(route.Method))
		allowed := operation == "post" && (method == "POST" || method == "PATCH") || operation == "patch" && (method == "PATCH" || method == "PUT" || method == "DELETE") || operation == "put" && (method == "PUT" || method == "DELETE")
		if !allowed {
			return fmt.Errorf("internal mutation root operation %s is incompatible with %s route", operation, method)
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
