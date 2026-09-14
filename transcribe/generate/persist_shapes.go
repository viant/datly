package generate

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	xshape "github.com/viant/x/shape"
)

// shapeDestinations identifies generated-owned contracts and row shapes.
// Handler, router and template artifacts retain fingerprint protection.
func (p *scaffoldPersistence) shapeDestinations() map[string]bool {
	destinations := map[string]bool{}
	if p.plan == nil {
		return destinations
	}
	if p.plan.Input.Ownership == ContractGenerated {
		destinations[p.plan.Input.Destination] = true
	}
	if p.plan.Output.Ownership == ContractGenerated {
		destinations[p.plan.Output.Destination] = true
	}
	for _, view := range p.plan.Views {
		if view.Ownership == ViewGenerated {
			destinations[view.Destination] = true
		}
	}
	if p.plan.hasViewSupport() {
		destinations[p.plan.ViewDest] = true
	}
	for _, shape := range p.plan.GeneratedTypes {
		destinations[shape.Destination] = true
	}
	return destinations
}

func (p *scaffoldPersistence) mergeShapes(target, existing string, manifest *scaffoldManifest) error {
	destinations := p.shapeDestinations()
	p.customizedShapes = map[string]bool{}
	p.fieldOwnership = map[string]*projectionFieldOwnership{}
	for name, fields := range manifest.ProjectionFields {
		p.fieldOwnership[name] = fields
	}
	proposals := map[string]string{}
	for _, file := range p.proposal {
		proposals[file.Path] = file.Content
	}
	for i := range p.files {
		file := &p.files[i]
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		if !destinations[relative] {
			continue
		}
		proposal, found := proposals[file.Path]
		if !found {
			return fmt.Errorf("raw generated shape proposal missing for %s", relative)
		}
		previous, err := os.ReadFile(filepath.Join(existing, relative))
		if os.IsNotExist(err) {
			if _, err = p.projectionChanges(relative, nil, []byte(proposal), manifest); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if fingerprint := manifest.Fingerprints[relative]; fingerprint != "" {
			p.customizedShapes[relative] = fingerprint != scaffoldFingerprint(previous)
		} else {
			p.customizedShapes[relative] = !bytes.Equal(previous, []byte(file.Content))
		}
		edits, err := p.projectionChanges(relative, previous, []byte(proposal), manifest)
		if err != nil {
			return err
		}
		merged, err := (xshape.SourceParser{}).EditStructFields(previous, []byte(file.Content), edits)
		if err != nil {
			return fmt.Errorf("update generated shape %s: %w", relative, err)
		}
		file.Content = string(merged)
	}
	return nil
}

func (p *scaffoldPersistence) retainShapes(target, existing string, manifest *scaffoldManifest, desired []string) ([]string, map[string]string, error) {
	shapes := p.shapeDestinations()
	roles := map[string]string{}
	for _, file := range desired {
		role := "artifact"
		if shapes[file] {
			role = "shape"
		}
		roles[file] = role
	}
	removals := map[string]bool{}
	for _, file := range p.removals {
		relative, err := managedRelativePath(file)
		if err != nil {
			return nil, nil, err
		}
		removals[relative] = true
	}
	for _, file := range manifest.Files {
		relative, err := managedRelativePath(file)
		if err != nil {
			return nil, nil, err
		}
		if _, found := roles[relative]; found || removals[relative] {
			continue
		}
		role := manifest.Roles[relative]
		if role == "" {
			if filepath.Ext(relative) != ".go" {
				return nil, nil, fmt.Errorf("unclassified obsolete generated artifact %s: migrate its manifest role before removal", relative)
			}
			content, err := os.ReadFile(filepath.Join(existing, relative))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			kind, err := (xshape.SourceParser{}).Classify(content)
			if err != nil {
				return nil, nil, err
			}
			switch kind {
			case xshape.SourceShape:
				role = "shape"
			case xshape.SourceProgram:
				role = "artifact"
			default:
				return nil, nil, fmt.Errorf("ambiguous obsolete generated Go file %s: migrate its manifest role before removal", relative)
			}
		}
		if role == "shape" {
			desired = append(desired, relative)
			roles[relative] = role
		} else if role != "artifact" {
			return nil, nil, fmt.Errorf("unsupported generated artifact role %q for %s", role, relative)
		}
	}
	sort.Strings(desired)
	return desired, roles, nil
}

// shapeTypeUpdates carries CAST and canonical relation authority only to their
// generated-owned destination, type and field. Relation changes additionally
// require the exact prior generated type and tag; CAST keeps its authored policy.
func (p *scaffoldPersistence) shapeTypeUpdates(destination string, existing, owned map[string]projectionField) ([]xshape.SourceFieldTypeUpdate, error) {
	if p.plan == nil {
		return nil, nil
	}
	var result []xshape.SourceFieldTypeUpdate
	for _, view := range p.plan.Views {
		if view.Ownership != ViewGenerated || view.Destination != destination {
			continue
		}
		for _, field := range view.Fields {
			if field.RelationHolder {
				key := view.Name + "." + field.Name
				before, present := existing[key]
				if !present {
					continue
				}
				prior, trusted := owned[key]
				if trusted && prior.Type == field.Type || !trusted && before.Type == field.Type {
					continue // No cardinality edit; retain native type/tag conflict checks.
				}
				if !trusted {
					return nil, fmt.Errorf("shape %s has no trustworthy generated field ownership for %s: explicit ownership migration required before relation cardinality change", destination, key)
				}
				if before.Type != prior.Type || before.Tag != prior.Tag || field.Tag != prior.Tag {
					return nil, fmt.Errorf("shape %s field %s has customized type or tag: explicit migration required before relation cardinality change", destination, key)
				}
				// Both expressions come from canonical generated field ownership and
				// the relation planner. Only the slice wrapper may change, never T.
				if prior.Type != "[]"+field.Type && field.Type != "[]"+prior.Type {
					return nil, fmt.Errorf("shape %s field %s changes relation child type: explicit migration required", destination, key)
				}
				result = append(result, xshape.SourceFieldTypeUpdate{Owner: view.Name, Field: field.Name, TypeExpr: field.Type})
				continue
			}
			if field.ExplicitType {
				result = append(result, xshape.SourceFieldTypeUpdate{Owner: view.Name, Field: field.Name, TypeExpr: field.Type})
			}
		}
	}
	if destination == p.plan.ViewDest {
		for _, helper := range p.plan.HelperTypes {
			for _, field := range helper.Fields {
				if field.ExplicitType {
					result = append(result, xshape.SourceFieldTypeUpdate{Owner: helper.Name, Field: field.Name, TypeExpr: field.Type})
				}
			}
		}
	}
	return result, nil
}
