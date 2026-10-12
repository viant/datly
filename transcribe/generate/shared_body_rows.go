package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	sqlio "github.com/viant/sqlx/io"
)

// Current owns its query and projection, while an explicitly selected generated
// body owns the shared Go row. Current must never widen that body or its Has type.
func (p *viewPlanner) shareBodyRow(current, owner *spec.View, destination string) error {
	ownerType, err := p.plan.CanonicalType(p.plan.Package, p.names[owner])
	if err != nil {
		return err
	}
	for _, reference := range p.readTypes[current] {
		canonical, err := p.plan.CanonicalType(p.plan.Package, reference)
		if err != nil {
			return err
		}
		if canonical != ownerType {
			return fmt.Errorf("shared Current %q type %q conflicts with body owner %q", current.Name, reference, ownerType)
		}
	}
	if len(current.Relations) != 0 || current.SelfReference != nil {
		return fmt.Errorf("shared Current %q cannot introduce body relations", current.Name)
	}
	if destination != p.dests[owner] {
		return fmt.Errorf("shared Current %q conflicts with body %q destination", current.Name, owner.Name)
	}
	owned, err := p.fields(owner)
	if err != nil {
		return err
	}
	columns := map[string]Field{}
	for _, field := range owned {
		mapping := sqlio.ParseTag(reflect.StructTag(field.Tag))
		if mapping.Transient || field.RelationHolder {
			continue
		}
		for _, column := range strings.Split(mapping.Column, "|") {
			if column == "" {
				continue
			}
			key := strings.ToLower(column)
			if previous, ok := columns[key]; ok && previous.Name != field.Name {
				return fmt.Errorf("ambiguous SQLX body mapping %q for shared Current %q", column, current.Name)
			}
			columns[key] = field
		}
	}
	excluded, err := sharedBodyTransientColumns(owner)
	if err != nil {
		return err
	}
	projected, err := resolveScalarViewFields(p.plan, current, p.velty)
	if err != nil {
		return err
	}
	for _, field := range projected {
		mapping := sqlio.ParseTag(reflect.StructTag(field.Tag))
		actual, ok := columns[strings.ToLower(mapping.Column)]
		if !mapping.Transient && !ok && excluded[strings.ToLower(mapping.Column)] {
			// Current consumes this physical value through its existing unmapped
			// resolver; the owning body deliberately excludes SQLX assignment.
			continue
		}
		if mapping.Transient || !ok {
			return fmt.Errorf("shared Current %q column %q has no SQLX field in body %q", current.Name, mapping.Column, owner.Name)
		}
		unknownBody := actual.Type == "any" && !actual.ExplicitType
		unknownRead := field.Type == "any" && !field.ExplicitType
		if unknownBody || unknownRead {
			// Before discovery, body columns have no physical type yet. An
			// explicit Current CAST is checked once body discovery completes.
			if !p.requireConcrete && (unknownBody || !field.ExplicitType) {
				continue
			}
			return fmt.Errorf("shared Current %q column %q has unresolved body/read type", current.Name, mapping.Column)
		}
		wantedType, err := p.plan.CanonicalType(p.plan.Package, field.Type)
		if err != nil {
			return err
		}
		actualType, err := p.plan.CanonicalType(p.plan.Package, actual.Type)
		if err != nil {
			return err
		}
		if !field.ExplicitType && strings.TrimPrefix(wantedType, "*") == strings.TrimPrefix(actualType, "*") {
			continue
		}
		if !field.ExplicitType && samePhysicalColumn(current, owner, mapping.Column) {
			continue
		}
		if wantedType != actualType {
			return fmt.Errorf("shared Current %q CAST column %q type %s conflicts with body field %s type %s", current.Name, mapping.Column, field.Type, actual.Name, actual.Type)
		}
	}
	return nil
}

// A body CAST may deliberately refine a physical column read by Current. This
// exception requires the same table, connector and database column type.
func samePhysicalColumn(current, owner *spec.View, name string) bool {
	if current.Source == nil || owner.Source == nil || current.Source.Table == "" || !strings.EqualFold(current.Source.Table, owner.Source.Table) {
		return false
	}
	connector := func(v *spec.View) string {
		if v.Source.Bindings != nil {
			return v.Source.Bindings.Connector
		}
		return ""
	}
	if connector(current) != connector(owner) {
		return false
	}
	find := func(v *spec.View) *spec.Column {
		var found *spec.Column
		for _, c := range v.Columns {
			if c == nil {
				continue
			}
			source := strings.TrimSpace(c.Source)
			if source == "" {
				source = strings.TrimSpace(c.Name)
			}
			mapping := sqlio.ParseTag(reflect.StructTag(scalarColumnFieldTag(c, source, false)))
			if mapping.Transient {
				continue
			}
			for _, physical := range strings.Split(mapping.Column, "|") {
				if !strings.EqualFold(physical, name) {
					continue
				}
				if found != nil && found != c {
					return nil
				}
				found = c
			}
		}
		return found
	}
	a, b := find(current), find(owner)
	return a != nil && b != nil && a.DatabaseType != "" && strings.EqualFold(a.DatabaseType, b.DatabaseType)
}

// Only explicit scalar exclusions with physical source identity qualify. Current
// cannot introduce an owner field or infer an exclusion from a Go field name.
func sharedBodyTransientColumns(owner *spec.View) (map[string]bool, error) {
	excluded := map[string]bool{}
	provenance := map[string]string{}
	for _, column := range owner.Columns {
		if column == nil {
			continue
		}
		source := strings.TrimSpace(column.Source)
		if source == "" || (strings.TrimSpace(column.Expression) != "" && !strings.EqualFold(strings.TrimSpace(column.Expression), source)) {
			continue
		}
		parts, err := sqlparser.TableIdentifierParts(source)
		if err != nil || len(parts) != 1 {
			continue
		}
		key := strings.ToLower(parts[0])
		if previous, ok := provenance[key]; ok && previous != column.Name {
			return nil, fmt.Errorf("ambiguous body physical source %q for shared Current", source)
		}
		provenance[key] = column.Name
		mapping := sqlio.ParseTag(reflect.StructTag(scalarColumnFieldTag(column, source, false)))
		if mapping.Transient {
			excluded[key] = true
		}
	}
	return excluded, nil
}
