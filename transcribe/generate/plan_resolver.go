package generate

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

// planResolver owns one complete generation-plan resolution. It keeps
// component metadata, type authority, view indexes, and validation policy in
// one invocation-scoped object rather than threading them through globals.
type planResolver struct {
	input                  Input
	types                  typeResolver
	plan                   *Plan
	viewIndexes            map[string]int
	requireConcreteHelpers bool
}

func (r *planResolver) resolve() (*Plan, error) {
	r.input.Component = r.input.Component.Clone()
	if r.input.Component.Static != nil {
		return r.resolveStatic()
	}
	destinations := &shapeDestinations{input: &r.input, planningOnly: !r.requireConcreteHelpers}
	if err := destinations.prepare(); err != nil {
		return nil, err
	}
	if err := r.validateHandlerSelection(); err != nil {
		return nil, err
	}
	plan, err := r.resolveBase()
	if err != nil || plan == nil {
		return plan, err
	}
	r.plan = plan
	if err = r.validateHelperFieldNames(); err != nil {
		return nil, err
	}
	if err = r.validateViewReferences(); err != nil {
		return nil, err
	}
	if err = r.validateViewBindings(); err != nil {
		return nil, err
	}
	if r.viewIndexes, err = r.resolveViews(); err != nil {
		return nil, err
	}
	if err = r.applySetMarkerViews(); err != nil {
		return nil, err
	}
	if err = r.concretizeFields(r.plan.Input.Fields); err != nil {
		return nil, err
	}
	if err = r.concretizeFields(r.plan.Output.Fields); err != nil {
		return nil, err
	}
	if err = r.bindIndependentViewFields(r.plan.Input.Fields); err != nil {
		return nil, err
	}
	if err = r.concretizeGeneratedHelperFields(); err != nil {
		return nil, err
	}
	if r.types != nil {
		if err = r.concretizeHelperFields(); err != nil {
			return nil, err
		}
	}
	if r.requireConcreteHelpers {
		if err = r.validateConcreteHelperFields(); err != nil {
			return nil, err
		}
	}
	if err = r.resolveContractOwnership(); err != nil {
		return nil, err
	}
	destinations.contracts(r.plan)
	if r.input.VeltyHandler != nil {
		applyGeneratedInputVeltyAliases(r.plan, r.input.Component)
	}
	if err = resolveGoHandler(r.plan, r.input.GoHandler, r.input.TargetPackage); err != nil {
		return nil, err
	}
	if err = resolveContractHandler(r.plan, r.input.ContractHandler, r.input.TargetPackage); err != nil {
		return nil, err
	}
	if err = r.input.MutationHandler.resolve(r.plan, r.input.TargetPackage); err != nil {
		return nil, err
	}
	r.resolveFactoryLink()
	if err = resolveHookScaffold(r.plan, r.input.HookScaffold); err != nil {
		return nil, err
	}
	if err = resolveVeltyHandler(r.plan, r.input.VeltyHandler); err != nil {
		return nil, err
	}
	r.plan.GeneratedTypes, err = resolveGeneratedTypes(r.input.GeneratedTypes, r.input.TargetPackage, r.types)
	if err != nil {
		return nil, err
	}
	// Shape-only planning must not materialize package assets. Runtime input
	// discovery has no resource authority; the generation plan owns embedding.
	if r.requireConcreteHelpers {
		r.plan.Resources, err = r.prepareResources()
		if err != nil {
			return nil, err
		}
	}
	if err = r.resolveEntitySupport(); err != nil {
		return nil, err
	}
	if err = r.plan.validateGeneratedNames(); err != nil {
		return nil, err
	}
	if err = r.plan.validateGeneratedDestinations(); err != nil {
		return nil, err
	}
	if err = destinations.partition(r.plan); err != nil {
		return nil, err
	}
	return r.plan, nil
}

func (r *planResolver) validateHandlerSelection() error {
	count := 0
	for _, present := range []bool{r.input.GoHandler != nil, r.input.ContractHandler != nil, r.input.MutationHandler != nil, r.input.VeltyHandler != nil} {
		if present {
			count++
		}
	}
	if count > 1 {
		return fmt.Errorf("custom Go, generated contract, mutation definition, and Velty handler assets are mutually exclusive")
	}
	return nil
}

func (r *planResolver) validateViewReferences() error {
	component := r.input.Component
	for path := range r.input.Views {
		if path == RootViewPath {
			continue
		}
		if component == nil {
			return fmt.Errorf("linked view %q requires canonical component metadata", path)
		}
		matched := false
		for _, view := range component.Views {
			identity, err := view.Identity()
			if err != nil {
				return err
			}
			if identity == path {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("linked view %q has no canonical independent view", path)
		}
	}
	if _, linkedRoot := r.input.Views[RootViewPath]; !linkedRoot {
		return nil
	}
	if r.input.Contracts.Output == nil {
		return fmt.Errorf("linked root view requires a linked output contract")
	}
	if component == nil || component.RootView == nil {
		return fmt.Errorf("linked root view requires canonical root view metadata")
	}
	return nil
}

func (r *planResolver) validateViewBindings() error {
	if len(r.input.ViewBindings) == 0 {
		return nil
	}
	component := r.input.Component
	params := map[string]*spec.Parameter{}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param != nil && strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			params[param.Identity()] = param
		}
	}
	views := map[string]*spec.View{}
	for _, view := range component.Views {
		if view == nil {
			continue
		}
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		views[identity] = view
	}
	for paramIdentity, viewIdentity := range r.input.ViewBindings {
		param := params[paramIdentity]
		if param == nil {
			return fmt.Errorf("independent view binding parameter %q is not canonical", paramIdentity)
		}
		view := views[viewIdentity]
		if view == nil {
			return fmt.Errorf("independent view binding %q targets unknown canonical view %q", paramIdentity, viewIdentity)
		}
		targetName := strings.TrimSpace(param.Source.Name)
		if targetName == "" {
			targetName = strings.TrimSpace(param.Name)
		}
		if !strings.EqualFold(strings.TrimSpace(view.CanonicalName()), targetName) {
			return fmt.Errorf("independent view binding %q targets view %q instead of %q", paramIdentity, viewIdentity, targetName)
		}
	}
	return nil
}

func (r *planResolver) lookup() func(string) (*x.Type, error) {
	if r.types == nil {
		return nil
	}
	return r.types.Descriptor
}

func (r *planResolver) rootSource() string {
	view := r.input.Component.RootView
	if view == nil || view.Source == nil {
		return ""
	}
	return strings.TrimSpace(view.Source.URI)
}

func (r *planResolver) rootConnector() string {
	view := r.input.Component.RootView
	if view == nil || view.Source == nil || view.Source.Bindings == nil {
		return ""
	}
	return strings.TrimSpace(view.Source.Bindings.Connector)
}
