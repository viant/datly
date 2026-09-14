package generate

import (
	"bytes"
	"fmt"
	"reflect"

	sqlio "github.com/viant/sqlx/io"
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
	for _, candidate := range desired {
		before, present := existing[candidate.key()]
		if !present {
			continue
		}
		requested := sqlio.ParseTag(reflect.StructTag(candidate.Tag)).Required
		actual := sqlio.ParseTag(reflect.StructTag(before.Tag)).Required
		if requested == actual {
			continue
		}
		prior, trusted := owned[candidate.key()]
		if !trusted || !requested {
			return xshape.SourceFieldEdits{}, fmt.Errorf("shape %s field %s requires explicit required-constraint tag migration", destination, candidate.key())
		}
		updated := tags.NewTags(prior.Tag)
		sqlxTag := updated.Lookup(sqlio.TagSqlx)
		if sqlxTag == nil || hasSQLXOption(sqlxTag.Values, "required") {
			return xshape.SourceFieldEdits{}, fmt.Errorf("shape %s field %s has conflicting required-constraint tag authority", destination, candidate.key())
		}
		sqlxTag.Append("required=true")
		if updated.Stringify() != candidate.Tag {
			return xshape.SourceFieldEdits{}, fmt.Errorf("shape %s field %s changes unrelated tags with its required constraint", destination, candidate.key())
		}
		edits.Tags = append(edits.Tags, xshape.SourceFieldTagUpdate{Owner: candidate.Owner, Field: candidate.Name, Previous: prior.Tag, Tag: candidate.Tag})
	}
	for _, field := range ownership.Fields {
		if desired, retained := current[field.key()]; retained {
			if bindings[field.key()] && field.Tag != desired.Tag {
				codecOnly, err := field.codecChange(desired)
				if err != nil {
					return xshape.SourceFieldEdits{}, err
				}
				if codecOnly {
					edits.Tags = append(edits.Tags, xshape.SourceFieldTagUpdate{Owner: field.Owner, Field: field.Name, Previous: field.Tag, Tag: desired.Tag})
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

// codecChange limits projection authority to its generated codec reference;
// unrelated binding, validation and application tags retain conflict protection.
func (f projectionField) codecChange(next projectionField) (bool, error) {
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
		if tag.Name != "codec" {
			return false, nil
		}
		changed = true
	}
	return changed, nil
}
