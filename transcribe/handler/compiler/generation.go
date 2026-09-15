package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// BuildInput derives mutation request state from the canonical writable graph.
// It returns detached metadata; authored parameters retain their authority.
func (c *Compiler) BuildInput(request Request, rootType string) (Request, error) {
	if request.Component == nil || request.Component.RootView == nil {
		return Request{}, fmt.Errorf("generation requires a canonical root view")
	}
	if strings.TrimSpace(rootType) == "" {
		return Request{}, fmt.Errorf("generation requires a resolved root type")
	}
	switch request.Operation {
	case plan.OperationPatch, plan.OperationPost, plan.OperationPut:
	default:
		return Request{}, fmt.Errorf("unsupported generation operation %q", request.Operation)
	}
	request.Component = request.Component.Clone()
	bindings := map[string]string{}
	for k, v := range request.ViewBindings {
		bindings[k] = v
	}
	request.ViewBindings = bindings
	request.Currents = append([]CurrentBinding(nil), request.Currents...)
	b := &inputGeneration{request: request, names: map[string]*spec.Parameter{}, visiting: map[string]bool{}}
	for _, p := range spec.EffectiveParameters(request.Component.Parameters) {
		if p != nil && !p.EmitOutput && p.Source.Kind != "output" {
			b.names[p.Name] = p
		}
	}
	var body *spec.Parameter
	for _, p := range spec.EffectiveParameters(request.Component.Parameters) {
		if p != nil && p.Source.Kind == "body" && !p.EmitOutput {
			if body != nil {
				return Request{}, fmt.Errorf("generation body input is ambiguous")
			}
			body = p
		}
	}
	if body == nil {
		name := typecatalog.FieldName(request.Component.Name)
		required := true
		expression, cardinality := "[]*"+rootType, "Many"
		if request.Component.RootView.Cardinality == spec.CardinalityOne {
			expression, cardinality = "*"+rootType, "One"
		}
		body = &spec.Parameter{Name: name, Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: expression, Cardinality: cardinality, Required: &required}
		if err := b.append(body); err != nil {
			return Request{}, err
		}
	}
	if request.Input != "" && request.Input != body.Name {
		return Request{}, fmt.Errorf("generation input %q conflicts with body %q", request.Input, body.Name)
	}
	b.request.Input = body.Name
	output, err := (&compiler{}).selectOutput(b.request.Component, request.Output)
	if err != nil {
		return Request{}, err
	}
	if output == nil {
		output = &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: body.TypeExpr, OutputTypeExpr: body.OutputTypeExpr, Cardinality: body.Cardinality}
		b.request.Component.Parameters = append(b.request.Component.Parameters, output)
	}
	b.request.Output = output.Name
	if request.Operation == plan.OperationPatch || request.Operation == plan.OperationPut {
		if err := b.record(b.request.Component.RootView, body.Name, nil, nil); err != nil {
			return Request{}, err
		}
	}
	return b.request, nil
}

type inputGeneration struct {
	request  Request
	names    map[string]*spec.Parameter
	visiting map[string]bool
}

func (b *inputGeneration) append(p *spec.Parameter) error {
	if prior := b.names[p.Name]; prior != nil {
		return fmt.Errorf("generated parameter %q conflicts with authored %s/%s binding", p.Name, prior.Source.Kind, prior.Source.Name)
	}
	b.names[p.Name] = p
	b.request.Component.Parameters = append(b.request.Component.Parameters, p)
	return nil
}

func (b *inputGeneration) record(view *spec.View, body string, path []string, scope *currentParentScope) error {
	if view == nil {
		return fmt.Errorf("generation relation requires a view")
	}
	identity, err := view.Identity()
	if err != nil {
		return err
	}
	if b.visiting[identity] {
		return fmt.Errorf("generation writable view %q occurs in multiple roles; automatic current-state derivation requires distinct view identities", identity)
	}
	b.visiting[identity] = true
	keys, err := canonicalKeys(view)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("generation view %q requires discovered primary keys", identity)
	}
	currentName := ""
	if len(path) == 0 {
		currentName = b.request.Current
	}
	for _, binding := range b.request.Currents {
		if binding.ViewIdentity == identity {
			if currentName != "" && currentName != binding.Param {
				return fmt.Errorf("conflicting current bindings for %s", identity)
			}
			currentName = binding.Param
		}
	}
	if currentName == "" {
		currentName, err = b.currentName(view)
		if err != nil {
			return err
		}
	}

	if existing := b.names[currentName]; existing != nil {
		if existing.Source.Kind != "view" || b.request.ViewBindings[existing.Identity()] == "" {
			return fmt.Errorf("current input %q conflicts with authored binding", currentName)
		}
	} else {
		if scope != nil {
			if err := b.addScopedCurrent(view, currentName, scope); err != nil {
				return err
			}
		} else if err := b.addCurrent(view, body, path, currentName, keys); err != nil {
			return err
		}
	}
	found := false
	for _, binding := range b.request.Currents {
		if binding.ViewIdentity == identity {
			found = true
		}
	}
	if !found {
		b.request.Currents = append(b.request.Currents, CurrentBinding{ViewIdentity: identity, Param: currentName})
	}
	for _, rel := range view.Relations {
		if rel == nil {
			return fmt.Errorf("generation contains a nil relation")
		}
		if rel.Kind == spec.RelationKindDerived {
			continue
		}
		holder := rel.Holder
		if holder == "" {
			holder = rel.Name
		}
		next := append(append([]string(nil), path...), typecatalog.FieldName(holder))
		if rel.View != nil && rel.View.Auxiliary {
			if err := b.auxiliary(view, rel, body); err != nil {
				return err
			}
		} else if err := b.record(rel.View, body, next, &currentParentScope{view: view, relation: rel, input: currentName}); err != nil {
			return err
		}
	}
	return nil
}
