package generate

import (
	"fmt"
	sqlio "github.com/viant/sqlx/io"
	"go/ast"
	"go/parser"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// A linked package type is outside generated source ownership. CAST may agree
// with it, but cannot change its compiled fields or project a fictitious shape.
func (r *planResolver) validateLinkedViewCasts(view *spec.View, descriptor *x.Type) error {
	validator := &linkedViewCastValidator{plan: r.plan, shape: xshape.New(descriptor, r.types.Descriptor), lookup: r.types.Descriptor, discoveryOnly: r.input.columnDiscovery, visiting: map[*spec.View]bool{}}
	if h := r.input.ExternalHandler; h != nil && h.GeneratedContracts {
		visited := map[*spec.View]bool{}
		var contains func(*spec.View) bool
		contains = func(candidate *spec.View) bool {
			if candidate == nil || visited[candidate] {
				return false
			}
			if candidate == view {
				return true
			}
			visited[candidate] = true
			for _, relation := range candidate.Relations {
				if relation != nil && contains(relation.View) {
					return true
				}
			}
			return false
		}
		validator.factoryBody = contains(h.InputShape)
		validator.logicalOccurrence = h.compositionOccurrence
		if !validator.directNamedStruct(descriptor, map[string]bool{}) {
			return fmt.Errorf("linked factory type %s must be a named struct", descriptor.Key())
		}
	}
	return validator.validate(view, "")
}

type linkedViewCastValidator struct {
	plan              *Plan
	shape             *xshape.Type
	visiting          map[*spec.View]bool
	factoryBody       bool
	lookup            xshape.Lookup
	discoveryOnly     bool
	logicalOccurrence func(*spec.View) bool
}

// View relations supply authored holder paths; x/shape owns all traversal of
// the actual linked/synthetic type and its declaring package/import identity.
func (v *linkedViewCastValidator) validate(view *spec.View, prefix string) error {
	if view == nil {
		return nil
	}
	if v.visiting[view] {
		if v.factoryBody {
			return fmt.Errorf("linked factory body contains a relation cycle at %s", prefix)
		}
		return nil
	}
	if v.factoryBody && strings.TrimSpace(view.Dest) != "" {
		return fmt.Errorf("linked factory body %s cannot declare a generated destination", view.Name)
	}
	v.visiting[view] = true
	defer delete(v.visiting, view)
	fields, err := resolveScalarViewFields(v.plan, view, false)
	if err != nil {
		return err
	}
	imports := map[string]string{}
	for _, imported := range v.plan.Imports {
		imports[imported.Alias] = imported.Package
	}
	contract := xshape.Contract{Packages: xshape.Packages{Default: v.plan.Package, Imports: imports}}
	var markerNames []string
	for _, wanted := range fields {
		if !wanted.ExplicitType && !v.factoryBody {
			continue
		}
		path := prefix + wanted.Name
		mappedColumn := false
		// Linked Go names are authored. Resolve explicit native SQLX mappings
		// before applying the inferred generated name to the existing shape.
		wantedTag := sqlio.ParseTag(reflect.StructTag(wanted.Tag))
		if !wantedTag.Transient && wantedTag.Column != "" {
			linkedFields, err := v.shape.FieldsAt(strings.TrimSuffix(prefix, "."))
			if err != nil {
				return err
			}
			mapped := ""
			bestMatch, ambiguous := 0, false
			for _, field := range linkedFields {
				if !field.Exported {
					continue
				}
				fieldTag := sqlio.ParseTag(field.Tag)
				if fieldTag.Transient || fieldTag.Column == "" {
					continue
				}
				matched := 0
				for index, expected := range strings.Split(wantedTag.Column, "|") {
					for _, candidate := range strings.Split(fieldTag.Column, "|") {
						if strings.EqualFold(expected, candidate) {
							priority := 1
							// Native SQLX mappings keep the physical name first.
							// An explicit result alias distinguishes fields sharing it.
							if index > 0 {
								priority = 2
							}
							if priority > matched {
								matched = priority
							}
						}
					}
				}
				if matched == 0 || matched < bestMatch {
					continue
				}
				if matched == bestMatch {
					ambiguous = true
					continue
				}
				mapped, bestMatch, ambiguous = prefix+field.Name, matched, false
			}
			if ambiguous {
				return fmt.Errorf("ambiguous SQLX mapping %q on uneditable linked type %s", wantedTag.Column, v.shape.Descriptor().Key())
			}
			// A tableless source-member projection names an existing Go field,
			// rather than a physical SQLX column. Keep its exact declared acronym
			// spelling; generation naming policy cannot rename a linked struct.
			if mapped == "" && v.factoryBody && v.logicalOccurrence != nil && v.logicalOccurrence(view) && !strings.Contains(wantedTag.Column, "|") {
				for _, field := range linkedFields {
					actualTag := sqlio.ParseTag(field.Tag)
					if field.Exported && field.Name == wantedTag.Column && actualTag.Column == "" && !actualTag.Transient {
						mapped = prefix + field.Name
						break
					}
				}
			}
			if mapped != "" {
				path = mapped
				mappedColumn = true
			}
		}
		actual, err := v.shape.ResolveField(path)
		if err != nil {
			return fmt.Errorf("CAST field %s on uneditable linked type %s: %w", path, v.shape.Descriptor().Key(), err)
		}
		if v.factoryBody && !mappedColumn && !wantedTag.Transient && wantedTag.Column != "" {
			actualFields, err := v.shape.FieldsAt(strings.TrimSuffix(prefix, "."))
			if err != nil {
				return err
			}
			for _, field := range actualFields {
				if field.Name != strings.TrimPrefix(path, prefix) {
					continue
				}
				actualTag := sqlio.ParseTag(field.Tag)
				if !actualTag.Transient && actualTag.Column != "" {
					return fmt.Errorf("linked factory body field %s SQLX mapping %q does not match projected %q", path, actualTag.Column, wantedTag.Column)
				}
			}
		}
		if v.factoryBody && !wanted.ExplicitType && wanted.Type == "any" {
			if !v.discoveryOnly {
				return fmt.Errorf("linked factory body field %s requires a discovered concrete type", path)
			}
			markerNames = append(markerNames, strings.TrimPrefix(path, prefix))
			continue
		}
		equivalent, err := contract.Equivalent(wanted.Type, actual.Identity)
		if err != nil {
			return err
		}
		if !equivalent {
			return fmt.Errorf("CAST cannot change uneditable linked field %s.%s: declared %s differs from linked field %s", v.shape.Descriptor().Key(), path, wanted.Type, actual.Identity)
		}
		if v.factoryBody {
			markerNames = append(markerNames, strings.TrimPrefix(path, prefix))
		}
	}
	for _, relation := range view.Relations {
		if relation == nil || relation.View == nil {
			continue
		}
		holder := typecatalog.FieldName(relation.Holder)
		if holder == "" {
			holder = typecatalog.FieldName(relation.Name)
		}
		if v.factoryBody {
			actual, err := v.shape.ResolveField(prefix + holder)
			if err != nil {
				return fmt.Errorf("linked factory body holder %s: %w", prefix+holder, err)
			}
			wrapper := "[]*"
			if relation.Cardinality == spec.CardinalityOne {
				wrapper = "*"
			}
			identity := actual.Identity
			element := strings.TrimPrefix(identity, wrapper)
			if !strings.HasPrefix(identity, wrapper) || strings.HasPrefix(element, "*") || strings.HasPrefix(element, "[") {
				return fmt.Errorf("linked factory body holder %s has incompatible cardinality: %s", prefix+holder, identity)
			}
			if !v.directNamedStruct(actual.Descriptor, map[string]bool{}) {
				return fmt.Errorf("linked factory body holder %s element %s must be a named struct", prefix+holder, element)
			}
			if expression := strings.TrimSpace(relation.View.TypeName); expression != "" {
				equal, err := contract.Equivalent(wrapper+expression, identity)
				if err != nil || !equal {
					return fmt.Errorf("linked factory body holder %s type %s does not match %s: %v", prefix+holder, identity, wrapper+expression, err)
				}
			}
			markerNames = append(markerNames, holder)
		}
		if err := v.validate(relation.View, prefix+holder+"."); err != nil {
			return err
		}
	}
	if v.factoryBody {
		return v.validatePresence(view, prefix, markerNames)
	}
	return nil
}

// Linked rows retain their actual presence provider; generation must not invent
// a replacement Has contract or silently discard sparse zero/null membership.
func (v *linkedViewCastValidator) validatePresence(view *spec.View, prefix string, names []string) error {
	fields, err := v.shape.FieldsAt(strings.TrimSuffix(prefix, "."))
	if err != nil {
		return err
	}
	marker := ""
	for _, field := range fields {
		if !field.Exported || field.Tag.Get("setMarker") != "true" && !sqlio.ParseTag(field.Tag).PresenceProvider {
			continue
		}
		if marker != "" {
			return fmt.Errorf("linked factory body %s has ambiguous presence providers", prefix)
		}
		marker = field.Name
		if !v.directStructMarker(field) {
			return fmt.Errorf("linked factory body %s presence provider must be a pointer to a struct", prefix)
		}
	}
	if marker == "" {
		if v.logicalOccurrence != nil && v.logicalOccurrence(view) {
			return nil
		}
		return fmt.Errorf("linked factory body %s requires an existing sparse presence provider", prefix)
	}
	// Decoding uses the external contract, not just this SQL projection. Retain
	// presence for public logical/transient fields even when SQL omits them.
	for _, field := range fields {
		if !field.Exported || field.Name == marker || strings.Split(field.Tag.Get("json"), ",")[0] == "-" || field.Tag.Get("internal") == "true" {
			continue
		}
		names = append(names, field.Name)
	}
	markerFields, err := v.shape.FieldsAt(prefix + marker)
	if err != nil {
		return fmt.Errorf("linked factory body presence provider %s: %w", prefix+marker, err)
	}
	available := map[string]string{}
	for _, field := range markerFields {
		if !field.Exported {
			continue
		}
		identity, err := field.CanonicalType()
		if err != nil {
			return err
		}
		available[field.Name] = identity
	}
	for _, name := range names {
		if available[name] != "bool" {
			return fmt.Errorf("linked factory body presence %s%s.%s must be bool", prefix, marker, name)
		}
	}
	return nil
}

// FieldsAt follows structural containers for ordinary field traversal. Presence
// providers need a stricter boundary: exactly one pointer to a struct, including
// source-only named aliases, without normalizing named collections or pointers.
func (v *linkedViewCastValidator) directStructMarker(field xshape.Field) bool {
	if field.ReflectedType != nil {
		return field.ReflectedType.Kind() == reflect.Pointer && field.ReflectedType.Elem().Kind() == reflect.Struct
	}
	identity, err := field.CanonicalType()
	if err != nil || !strings.HasPrefix(identity, "*") || strings.HasPrefix(identity, "**") || strings.HasPrefix(identity, "*[") {
		return false
	}
	if expression, err := parser.ParseExpr(field.TypeExpr); err == nil {
		if pointer, ok := expression.(*ast.StarExpr); ok {
			if _, ok := pointer.X.(*ast.StructType); ok {
				return true
			}
		}
	}
	if v.lookup == nil {
		return false
	}
	descriptor, err := v.lookup(strings.TrimPrefix(identity, "*"))
	return err == nil && v.directNamedStruct(descriptor, map[string]bool{})
}

func (v *linkedViewCastValidator) directNamedStruct(descriptor *x.Type, seen map[string]bool) bool {
	if descriptor == nil || descriptor.Name == "" || descriptor.PkgPath == "" || seen[descriptor.Key()] {
		return false
	}
	seen[descriptor.Key()] = true
	if descriptor.Type != nil {
		return descriptor.Type.Kind() == reflect.Struct
	}
	declaration := descriptor.SynteticType
	if declaration == nil || declaration.TypeSpec == nil {
		return false
	}
	switch expression := declaration.TypeSpec.Type.(type) {
	case *ast.StructType:
		return true
	case *ast.Ident, *ast.SelectorExpr:
		imports := map[string]string{}
		for alias, imported := range declaration.Imports {
			if imported != nil {
				imports[alias] = imported.Path
			}
		}
		canonical, err := canonicalType(expression, imports, descriptor.PkgPath)
		if err != nil || v.lookup == nil {
			return false
		}
		target, err := v.lookup(canonical)
		return err == nil && v.directNamedStruct(target, seen)
	default:
		return false
	}
}
