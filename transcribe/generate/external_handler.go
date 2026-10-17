package generate

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/gobuild"
	"go/token"
)

// ExternalHandler links an application-validated factory without copying its
// implementation or generating replacement contracts.
type ExternalHandler struct {
	// ValidateOwnerSources rechecks compilation-local linked owner source/schema
	// evidence against original bytes and, when supplied, their staged mapping.
	// It emits no receipt and grants no generated-file or runtime authority.
	ValidateOwnerSources func(mapPath func(string) string) error `json:"-" yaml:"-"`

	// SourceMemberFiles reuses the current build-selected parse; no persisted ownership registry.
	SourceMemberFiles map[string]SourceMemberFile
	// CompositionViews is exact compiler-established occurrence evidence, never serialized.
	CompositionViews map[string]CompositionOccurrence `json:"-" yaml:"-"`
	// GeneratedContracts is the canonical source factory branch; the field planner owns local Input/Output.
	GeneratedContracts bool
	// InputShape is compiler-owned authoring metadata. It only owns generated fields;
	// it never belongs to Component.RootView/Views or executable route metadata.
	InputShape *spec.View
	Package    string
	Name       string
	// AdapterPackage/AdapterName override runtime adaptation while Package/Name
	// retain the contract factory's canonical registration identity.
	AdapterPackage string
	AdapterName    string
	// Build validates source-authored factories and staged registrations without
	// requiring their contracts to be linked into the transcription executable.
	Build *gobuild.Context
	// WireTypes retains canonical Go-owner discovery references for linked Input.
	WireTypes []string
}

func (h *ExternalHandler) Clone() *ExternalHandler {
	if h == nil {
		return nil
	}
	result := *h
	if h.CompositionViews != nil {
		result.CompositionViews = make(map[string]CompositionOccurrence, len(h.CompositionViews))
		for identity, proof := range h.CompositionViews {
			result.CompositionViews[identity] = proof
		}
	}
	result.Build = h.Build.Clone()
	result.WireTypes = append([]string(nil), h.WireTypes...)
	result.InputShape = h.InputShape.Clone()
	return &result
}

func (r *planResolver) resolveExternalHandler() error {
	h := r.input.ExternalHandler
	if h == nil {
		return nil
	}
	if (h.AdapterPackage == "") != (h.AdapterName == "") || h.AdapterName != "" && (!token.IsIdentifier(h.AdapterName) || !token.IsExported(h.AdapterName)) {
		return fmt.Errorf("external handler adapter requires a package and exported factory")
	}
	if h.GeneratedContracts {
		if h.AdapterName != "" {
			return fmt.Errorf("handler adapter requires source-authored contracts separate from the generated destination")
		}
		if h.Package != r.plan.Package || !token.IsIdentifier(h.Name) || !token.IsExported(h.Name) || r.plan.Input.Ownership != ContractGenerated || r.plan.Output.Ownership != ContractGenerated {
			return fmt.Errorf("generated POST factory requires local generated contracts and an exported factory")
		}
	} else if h.Package == "" || !token.IsIdentifier(h.Name) || !token.IsExported(h.Name) || r.plan.Input.Ownership != ContractLinked || r.plan.Output.Ownership != ContractLinked {
		return fmt.Errorf("external handler requires imported contracts and an exported factory")
	}
	if r.input.Component.RootView != nil || (!h.GeneratedContracts && len(r.input.Component.Views) != 0) {
		return fmt.Errorf("external handler registration cannot contain reader views")
	}
	if r.plan.Handler != h.Package+"."+h.Name {
		return fmt.Errorf("external handler factory conflicts with route handler")
	}
	if h.GeneratedContracts {
		r.plan.ExternalHandler = h.Clone()
		r.plan.FactoryExpression = h.Name
		return nil
	}
	alias := uniqueImportAlias(r.plan, h.Package)
	ensureImport(r.plan, alias, h.Package)
	r.plan.ExternalHandler = h.Clone()
	r.plan.FactoryExpression = alias + "." + h.Name
	if h.AdapterName != "" {
		adapterAlias := uniqueImportAlias(r.plan, h.AdapterPackage)
		ensureImport(r.plan, adapterAlias, h.AdapterPackage)
		r.plan.AdapterExpression = adapterAlias + "." + h.AdapterName
	}
	return nil
}

// CompositionOccurrence is compilation-local evidence from the existing selected
// source-member composition proof. It grants no runtime or generated ownership.
type CompositionOccurrence struct {
	TypeName    string
	Cardinality spec.Cardinality
}

func (h *ExternalHandler) compositionOccurrence(view *spec.View) bool {
	if h == nil || !h.GeneratedContracts || view == nil {
		return false
	}
	identity, err := view.Identity()
	if err != nil {
		return false
	}
	proof, ok := h.CompositionViews[identity]
	return ok && proof.TypeName != "" && (proof.Cardinality == spec.CardinalityOne || proof.Cardinality == spec.CardinalityMany) && proof.TypeName == view.TypeName && proof.Cardinality == view.Cardinality && view.Source != nil && view.Source.Table == "" && view.Source.SQL == "" && view.Source.URI == "" && len(view.Source.Embeds) == 0 && view.Source.Controls == nil && view.Source.Bindings == nil && view.SelfReference == nil && view.RowLock == ""
}

func (h *ExternalHandler) validateCompositionOccurrences() error {
	if h == nil || len(h.CompositionViews) == 0 {
		return nil
	}
	if !h.GeneratedContracts || h.InputShape == nil {
		return fmt.Errorf("composition occurrence proof requires a generated factory body")
	}
	found := map[string]bool{}
	visiting := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visiting[view] {
			return fmt.Errorf("composition occurrence graph is incomplete or cyclic")
		}
		visiting[view] = true
		defer delete(visiting, view)
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		if _, exists := h.CompositionViews[identity]; exists {
			if found[identity] || !h.compositionOccurrence(view) {
				return fmt.Errorf("composition occurrence proof is stale, ambiguous or physical: %s", identity)
			}
			found[identity] = true
		}
		for _, relation := range view.Relations {
			if relation == nil {
				return fmt.Errorf("composition relation incomplete")
			}
			if err := visit(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(h.InputShape); err != nil {
		return err
	}
	if len(found) != len(h.CompositionViews) {
		return fmt.Errorf("composition occurrence proof contains an unknown member")
	}
	return nil
}
