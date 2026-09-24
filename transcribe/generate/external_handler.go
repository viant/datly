package generate

import (
	"fmt"
	"go/token"
)

// ExternalHandler links an application-validated factory without copying its
// implementation or generating replacement contracts.
type ExternalHandler struct {
	Package string
	Name    string
}

func (h *ExternalHandler) Clone() *ExternalHandler {
	if h == nil {
		return nil
	}
	result := *h
	return &result
}

func (r *planResolver) resolveExternalHandler() error {
	h := r.input.ExternalHandler
	if h == nil {
		return nil
	}
	if h.Package == "" || !token.IsIdentifier(h.Name) || !token.IsExported(h.Name) || r.plan.Input.Ownership != ContractLinked || r.plan.Output.Ownership != ContractLinked {
		return fmt.Errorf("external handler requires imported contracts and an exported factory")
	}
	if r.input.Component.RootView != nil || len(r.input.Component.Views) != 0 {
		return fmt.Errorf("external handler registration cannot contain reader views")
	}
	if r.plan.Handler != h.Package+"."+h.Name {
		return fmt.Errorf("external handler factory conflicts with route handler")
	}
	alias := uniqueImportAlias(r.plan, h.Package)
	ensureImport(r.plan, alias, h.Package)
	r.plan.ExternalHandler = h.Clone()
	r.plan.FactoryExpression = alias + "." + h.Name
	return nil
}
