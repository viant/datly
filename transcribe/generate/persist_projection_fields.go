package generate

import (
	"bytes"
	"fmt"

	"github.com/viant/tagly/tags"

	xshape "github.com/viant/x/shape"
)

// projectionFieldOwnership records only fields emitted by the generator for
// view projections, their presence shapes and declared projection helpers.
// Complete distinguishes exhaustive ownership evidence from an older file that
// cannot prove whether unlisted fields were generated or authored.
type projectionFieldOwnership struct {
	Complete bool              `json:"complete"`
	Fields   []projectionField `json:"fields"`
}

type projectionField struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Tag   string `json:"tag,omitempty"`
}

func (f projectionField) key() string { return f.Owner + "." + f.Name }

func (p *scaffoldPersistence) projectionOwners(destination string) map[string]bool {
	result := map[string]bool{}
	if p.plan == nil {
		return result
	}
	if p.plan.Input.Ownership == ContractGenerated && p.plan.localShape(p.plan.Input.Package) && destination == p.plan.Input.Destination {
		result[p.plan.Input.Type] = true
		if _, ok := hasMarkerField(p.plan.Input.Type, p.plan.Input.Fields); ok {
			result[p.plan.Input.Type+"Has"] = true
		}
	}
	if p.plan.Output.Ownership == ContractGenerated && p.plan.localShape(p.plan.Output.Package) && destination == p.plan.Output.Destination {
		result[p.plan.Output.Type] = true
	}
	for _, view := range p.plan.Views {
		if view.Ownership != ViewGenerated || view.Destination != destination {
			continue
		}
		result[view.Name] = true
		if len(view.SetMarkerFields) > 0 {
			result[view.Name+"Has"] = true
		}
	}
	if destination == p.plan.ViewDest {
		for _, helper := range p.plan.HelperTypes {
			result[helper.Name] = true
		}
	}
	return result
}

// projectionBindings identifies the generated input fields whose codec metadata
// refers to a currently authored projection helper, plus their presence mirrors.
func (p *scaffoldPersistence) projectionBindings(destination string) map[string]bool {
	result := map[string]bool{}
	if p.plan == nil || p.plan.Input.Ownership != ContractGenerated || destination != p.plan.Input.Destination {
		return result
	}
	helpers := map[string]bool{}
	for _, helper := range p.plan.HelperTypes {
		helpers[helper.Name] = true
	}
	for _, field := range p.plan.Input.Fields {
		if field.Source == "body" && tags.NewTags(field.Tag).Lookup("view") != nil {
			result[p.plan.Input.Type+"."+field.Name] = true
			if _, ok := hasMarkerField(p.plan.Input.Type, p.plan.Input.Fields); ok {
				result[p.plan.Input.Type+"Has."+field.Name] = true
			}
			continue
		}
		if field.Source != "param" {
			continue
		}
		reference, err := (xshape.Resolver{}).Reference(field.Type)
		if err != nil || reference.Qualifier != "" || !helpers[reference.BaseName] {
			continue
		}
		result[p.plan.Input.Type+"."+field.Name] = true
		if _, ok := hasMarkerField(p.plan.Input.Type, p.plan.Input.Fields); ok {
			result[p.plan.Input.Type+"Has."+field.Name] = true
		}
	}
	return result
}

// projectionFields uses the native source/import owner, never an emitter-local
// AST walker, to describe the actual raw generator proposal.
func (p *scaffoldPersistence) projectionFields(source []byte) ([]projectionField, error) {
	parsed, err := (xshape.SourceParser{}).Parse(source)
	if err != nil {
		return nil, err
	}
	imports := map[string]string{}
	for alias, imported := range parsed.Imports {
		imports[alias] = imported.Path
	}
	resolver := xshape.Resolver{Imports: imports}
	result := make([]projectionField, 0, len(parsed.Fields))
	seen := map[string]bool{}
	for _, field := range parsed.Fields {
		canonical, err := resolver.Canonical(field.TypeExpr)
		if err != nil {
			return nil, err
		}
		for _, name := range field.Names {
			value := projectionField{Owner: field.Owner, Name: name, Type: canonical, Tag: field.Tag}
			if seen[value.key()] {
				return nil, fmt.Errorf("duplicate shape field %s", value.key())
			}
			seen[value.key()] = true
			result = append(result, value)
		}
	}
	return result, nil
}

func (p *scaffoldPersistence) projectionChanges(destination string, previous, proposal []byte, manifest *scaffoldManifest) (xshape.SourceFieldEdits, error) {
	desired, err := p.projectionFields(proposal)
	if err != nil {
		return xshape.SourceFieldEdits{}, err
	}
	current := map[string]projectionField{}
	owners := p.projectionOwners(destination)
	bindings := p.projectionBindings(destination)
	for _, field := range desired {
		current[field.key()] = field
	}
	var before []projectionField
	if previous != nil {
		before, err = p.projectionFields(previous)
		if err != nil {
			return xshape.SourceFieldEdits{}, err
		}
	}
	existing := map[string]projectionField{}
	for _, field := range before {
		existing[field.key()] = field
	}
	ownership, known := manifest.ProjectionFields[destination]
	if ownership == nil {
		known = false
	}
	if !known {
		ownership = &projectionFieldOwnership{Complete: previous == nil}
		if previous != nil && (bytes.Equal(previous, proposal) || manifest.Fingerprints[destination] != "" && manifest.Fingerprints[destination] == scaffoldFingerprint(previous)) {
			ownership.Complete = true
			for _, field := range before {
				if owners[field.Owner] || bindings[field.key()] {
					ownership.Fields = append(ownership.Fields, field)
				}
			}
		}
	}
	// Older v5 manifests recorded generated input/output files as complete but
	// omitted their ordinary contract fields. When the entire current file still
	// matches its trusted generated fingerprint, backfill ownership from that
	// exact baseline so later DQL metadata changes can regenerate safely.
	if known && ownership.Complete && len(ownership.Fields) == 0 && previous != nil && manifest.Fingerprints[destination] != "" && manifest.Fingerprints[destination] == scaffoldFingerprint(previous) {
		for _, field := range before {
			if owners[field.Owner] || bindings[field.key()] {
				ownership.Fields = append(ownership.Fields, field)
			}
		}
	}
	owned := map[string]projectionField{}
	for _, field := range ownership.Fields {
		if field.Owner == "" || field.Name == "" || field.Type == "" {
			return xshape.SourceFieldEdits{}, fmt.Errorf("invalid projection ownership in %s", destination)
		}
		if _, exists := owned[field.key()]; exists {
			return xshape.SourceFieldEdits{}, fmt.Errorf("duplicate projection ownership %s in %s", field.key(), destination)
		}
		owned[field.key()] = field
	}
	if !ownership.Complete {
		for _, field := range before {
			if _, known := owned[field.key()]; known {
				continue
			}
			if !owners[field.Owner] && !bindings[field.key()] {
				continue
			}
			if _, retained := current[field.key()]; !retained {
				return xshape.SourceFieldEdits{}, fmt.Errorf("shape %s has no trustworthy generated field ownership for %s: explicit ownership migration required before projection removal", destination, field.key())
			}
		}
	}
	updates, err := p.shapeTypeUpdates(destination, existing, owned)
	if err != nil {
		return xshape.SourceFieldEdits{}, err
	}
	edits := xshape.SourceFieldEdits{Types: updates}
	relations := map[string]bool{}
	if p.plan != nil {
		for _, view := range p.plan.Views {
			if view.Destination == destination {
				for _, field := range view.Fields {
					if field.RelationHolder {
						relations[view.Name+"."+field.Name] = true
					}
				}
			}
		}
	}
	for _, candidate := range desired {
		before, present := existing[candidate.key()]
		if !present {
			continue
		}
		prior, trusted := owned[candidate.key()]
		if !owners[candidate.Owner] || !trusted || candidate.Tag == prior.Tag {
			continue
		}
		if relations[candidate.key()] {
			// Canonical outer aliases can change exact matching fields. This
			// grants no destination, source, cardinality or child-type authority.
			onlyOn, err := prior.tagChange(candidate, "on")
			if err != nil {
				return xshape.SourceFieldEdits{}, err
			}
			if !onlyOn || candidate.Type != prior.Type {
				continue
			}
		}
		// DQL controls generated projection metadata, but never adopts a
		// destination edit, even when that edit happens to equal the proposal.
		if before.Type != prior.Type || before.Tag != prior.Tag {
			return xshape.SourceFieldEdits{}, fmt.Errorf("shape %s field %s has customized type or tag: explicit migration required before metadata change", destination, candidate.key())
		}
		edits.Tags = append(edits.Tags, xshape.SourceFieldTagUpdate{Owner: candidate.Owner, Field: candidate.Name, Previous: prior.Tag, Tag: candidate.Tag})
	}
	for _, field := range ownership.Fields {
		if desired, retained := current[field.key()]; retained {
			if bindings[field.key()] && field.Tag != desired.Tag {
				for _, controlled := range []string{"codec", "view"} {
					controlledOnly, err := field.tagChange(desired, controlled)
					if err != nil {
						return xshape.SourceFieldEdits{}, err
					}
					if controlledOnly {
						edits.Tags = append(edits.Tags, xshape.SourceFieldTagUpdate{Owner: field.Owner, Field: field.Name, Previous: field.Tag, Tag: desired.Tag})
						break
					}
				}
			}
			continue
		}
		edits.Remove = append(edits.Remove, xshape.SourceFieldRemoval{Owner: field.Owner, Field: field.Name, TypeExpr: field.Type, Tag: field.Tag})
	}
	next := &projectionFieldOwnership{Complete: ownership.Complete, Fields: []projectionField{}}
	for _, field := range desired {
		if !owners[field.Owner] && !bindings[field.key()] {
			continue
		}
		_, wasOwned := owned[field.key()]
		_, wasPresent := existing[field.key()]
		if wasOwned || !wasPresent {
			next.Fields = append(next.Fields, field)
		}
	}
	p.fieldOwnership[destination] = next
	return edits, nil
}

// tagChange limits an owned transition to one canonical tag key.
// All other metadata retains its existing conflict protection.
func (f projectionField) tagChange(next projectionField, key string) (bool, error) {
	previous, err := tags.Parse(f.Tag)
	if err != nil {
		return false, err
	}
	desired, err := tags.Parse(next.Tag)
	if err != nil {
		return false, err
	}
	if len(previous) != len(desired) {
		return false, nil
	}
	changed := false
	for index, tag := range previous {
		other := desired[index]
		if tag.Name != other.Name {
			return false, nil
		}
		if tag.Values == other.Values {
			continue
		}
		if tag.Name != key {
			return false, nil
		}
		changed = true
	}
	return changed, nil
}
