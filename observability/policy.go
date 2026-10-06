package observability

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
)

// ProviderSchema selects one fixed built-in capture contract. Units and windows
// retain native gmetric semantics and cannot be overridden by a declaration.
type ProviderSchema string

const (
	Native14 ProviderSchema = "native14"
	Source11 ProviderSchema = "source11"
)

type OperationDescriptor struct {
	Name        string
	Location    string
	Description string
	Provider    ProviderSchema
}

// ViewObservation addresses existing compiled identities, never execution names.
type ViewObservation struct {
	Component    spec.Key
	ViewIdentity string
	Diagnostic   string
	Operation    *OperationDescriptor
}

// Policy is application configuration. Recorder construction freezes a deep copy.
type Policy struct{ Views []ViewObservation }

func (p *Policy) Copy() *Policy {
	if p == nil {
		return nil
	}
	result := &Policy{Views: append([]ViewObservation(nil), p.Views...)}
	for i := range result.Views {
		if op := result.Views[i].Operation; op != nil {
			copied := *op
			result.Views[i].Operation = &copied
		}
	}
	return result
}

type compiledPolicy struct {
	views      map[string]map[string]ViewObservation
	operations map[string]OperationDescriptor
}

func (p *Policy) compile() (*compiledPolicy, error) {
	r := &compiledPolicy{views: map[string]map[string]ViewObservation{}, operations: map[string]OperationDescriptor{}}
	if p == nil {
		return r, nil
	}
	for _, declaration := range p.Copy().Views {
		component := declaration.Component
		if component.Kind != spec.KindComponent || strings.TrimSpace(component.Name) == "" || strings.TrimSpace(declaration.ViewIdentity) == "" {
			return nil, fmt.Errorf("observation requires complete component key and effective view identity")
		}
		key := component.String()
		if r.views[key] == nil {
			r.views[key] = map[string]ViewObservation{}
		}
		if _, exists := r.views[key][declaration.ViewIdentity]; exists {
			return nil, fmt.Errorf("duplicate observation target %s %s", key, declaration.ViewIdentity)
		}
		if declaration.Operation != nil {
			op := *declaration.Operation
			if op.Provider == "" {
				op.Provider = Native14
			}
			if strings.TrimSpace(op.Name) == "" || strings.TrimSpace(op.Location) == "" || strings.TrimSpace(op.Description) == "" || op.Provider != Native14 && op.Provider != Source11 {
				return nil, fmt.Errorf("invalid observation operation for %s %s", key, declaration.ViewIdentity)
			}
			if previous, exists := r.operations[op.Name]; exists && previous != op {
				return nil, fmt.Errorf("conflicting observation operation %q", op.Name)
			}
			r.operations[op.Name] = op
			declaration.Operation = &op
		}
		r.views[key][declaration.ViewIdentity] = declaration
	}
	return r, nil
}

func (p *Policy) Validate() error { _, err := p.compile(); return err }

func WithPolicy(policy *Policy) RecorderOption {
	frozen := policy.Copy()
	return func(r *Recorder) { r.policy, r.policyErr = frozen.compile() }
}

// ViewTarget is emitted by completed reader plans for validation before use.
type ViewTarget struct {
	Component spec.Key
	View      *spec.View
}

type Resolution struct{ Diagnostic, Operation string }

// Resolve checks fallback collisions without registering a counter. Registration
// occurs at the first actual pending/timing/cache activity, not during staging.
func (r *Recorder) Resolve(component spec.Key, view *spec.View) (Resolution, error) {
	if r.policyErr != nil {
		return Resolution{}, r.policyErr
	}
	if view == nil {
		return Resolution{}, fmt.Errorf("view is required")
	}
	name := view.Name
	if strings.TrimSpace(name) == "" {
		name = "<root>"
	}
	fallback := OperationDescriptor{Name: component.String() + "/" + name, Location: "datly", Description: "view performance", Provider: Native14}
	result := Resolution{Diagnostic: name, Operation: fallback.Name}
	if r.policy != nil {
		var identity string
		if len(r.policy.views[component.String()]) > 0 {
			var err error
			identity, err = view.Identity()
			if err != nil {
				return Resolution{}, err
			}
		}
		if declaration, exists := r.policy.views[component.String()][identity]; exists {
			if declaration.Diagnostic != "" {
				result.Diagnostic = declaration.Diagnostic
			}
			if declaration.Operation != nil {
				result.Operation = declaration.Operation.Name
				return result, nil
			}
		}
		if declared, exists := r.policy.operations[fallback.Name]; exists && declared != fallback {
			return Resolution{}, fmt.Errorf("observation operation %q conflicts with native fallback", fallback.Name)
		}
	}
	return result, nil
}

// ValidateComponents uses the stage/index authority, without compiling views.
func (r *Recorder) ValidateComponents(components []spec.Key) error {
	if r.policyErr != nil {
		return r.policyErr
	}
	if r.policy == nil {
		return nil
	}
	available := map[string]bool{}
	for _, key := range components {
		available[key.String()] = true
	}
	var declared []string
	for key := range r.policy.views {
		declared = append(declared, key)
	}
	sort.Strings(declared)
	for _, key := range declared {
		if !available[key] {
			return fmt.Errorf("observation component target not found: %s", key)
		}
	}
	return nil
}

// ValidateTargets checks the union of reader and dependency plans in one
// materialized registration. Unrelated indexed components remain lazy.
func (r *Recorder) ValidateTargets(targets []ViewTarget, components ...spec.Key) error {
	seen := map[string]map[string]*spec.View{}
	for _, component := range components {
		seen[component.String()] = map[string]*spec.View{}
	}
	for _, target := range targets {
		if _, err := r.Resolve(target.Component, target.View); err != nil {
			return err
		}
		key := target.Component.String()
		// Unmapped native plans historically allow unnamed views and do not
		// need address validation. A policy must not tighten their contract.
		if r.policy == nil || len(r.policy.views[key]) == 0 {
			continue
		}
		identity, err := target.View.Identity()
		if err != nil {
			return err
		}
		if seen[key] == nil {
			seen[key] = map[string]*spec.View{}
		}
		if previous := seen[key][identity]; previous != nil && previous != target.View {
			return fmt.Errorf("ambiguous observation target %s %s", key, identity)
		}
		seen[key][identity] = target.View
		if _, err := r.Resolve(target.Component, target.View); err != nil {
			return err
		}
	}
	if r.policy != nil {
		var components []string
		for component := range seen {
			components = append(components, component)
		}
		sort.Strings(components)
		for _, component := range components {
			identities := seen[component]
			var declared []string
			for identity := range r.policy.views[component] {
				declared = append(declared, identity)
			}
			sort.Strings(declared)
			for _, identity := range declared {
				if identities[identity] == nil {
					return fmt.Errorf("observation view target not found: %s %s", component, identity)
				}
			}
		}
	}
	return r.policyErr
}
