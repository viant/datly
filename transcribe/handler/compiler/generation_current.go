package compiler

import (
	"fmt"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
	"strings"
)

func (b *inputGeneration) currentName(view *spec.View) (string, error) {
	name := "Current" + typecatalog.FieldName(view.CanonicalName())
	if b.names[name] != nil {
		return name, nil
	}
	var selected string
	for _, parameter := range spec.EffectiveParameters(b.request.Component.Parameters) {
		if parameter == nil || parameter.Source.Kind != "view" {
			continue
		}
		binding := b.request.ViewBindings[parameter.Identity()]
		for _, candidate := range b.request.Component.Views {
			if candidate == nil || candidate.Source == nil || view.Source == nil || candidate.Source.Table != view.Source.Table {
				continue
			}
			identity, err := candidate.Identity()
			if err != nil {
				return "", err
			}
			if identity != binding {
				continue
			}
			if selected != "" && selected != parameter.Name {
				return "", fmt.Errorf("generation current-state input for %s is ambiguous", view.CanonicalName())
			}
			selected = parameter.Name
		}
	}
	if selected != "" {
		name = selected
	}
	return name, nil
}

func (b *inputGeneration) addCurrent(view *spec.View, body string, path []string, currentName string, keys []plan.KeyPart) error {
	if view.Source == nil || view.Source.Table == "" {
		return fmt.Errorf("generation view %q requires a canonical table", view.CanonicalName())
	}
	keyName := typecatalog.FieldName(view.CanonicalName()) + "Keys"
	var projection []string
	for _, key := range keys {
		if key.Source == "" {
			return fmt.Errorf("generation key %s has no source column", key.Field)
		}
		alias := ""
		for _, col := range view.Columns {
			if col != nil && col.PrimaryKey && typecatalog.FieldName(col.Name) == key.Field {
				// StructQL helper projections are Go shapes. Keep the physical
				// database name in SQLX metadata and use the canonical exported
				// field name for the generated helper contract.
				alias = key.Field
				break
			}
		}
		if alias == "" {
			return fmt.Errorf("generation key %s has no projection", key.Field)
		}
		projection = append(projection, key.Field+" AS "+alias)
	}
	query := "SELECT " + strings.Join(projection, ", ") + " FROM `/" + strings.Join(path, "/") + "`"
	parameter := &spec.Parameter{Name: keyName, Source: spec.BindSource{Kind: "param", Name: body}, Cardinality: "Many", DeclarationSQL: query, Codec: &spec.Codec{Body: "structql", Args: []string{query}}}
	if err := b.appendProjection(parameter); err != nil {
		return err
	}
	return b.appendCurrent(view, currentName, "$criteria.CompositeIn(\"r\", $"+keyName+")")
}

func (b *inputGeneration) appendProjection(parameter *spec.Parameter) error {
	if existing := b.names[parameter.Name]; existing != nil {
		if existing.Source.Kind != "param" || existing.Source.Name != parameter.Source.Name || (existing.DeclarationSQL == "" && (existing.Codec == nil || existing.Codec.Body != "structql")) {
			return fmt.Errorf("key projection %q conflicts with authored binding", parameter.Name)
		}
	} else if err := b.append(parameter); err != nil {
		return err
	}
	return nil
}

func (b *inputGeneration) appendCurrent(view *spec.View, currentName, predicate string) error {
	current := view.Clone()
	current.Key.Name = currentName
	current.Name = currentName
	current.Namespace = ""
	current.TypeName = ""
	current.Dest = ""
	current.EntityHooks = ""
	current.Columns = nil
	for _, column := range view.Columns {
		if column == nil || column.DeleteMarker {
			continue
		}
		projected := column.Clone()
		projected.ConcurrencyToken = false
		current.Columns = append(current.Columns, projected)
	}
	current.Relations = nil
	current.SelfReference = nil
	current.Cardinality = spec.CardinalityMany
	sql := strings.TrimSpace(view.Source.SQL)
	if sql == "" {
		if view.Source.URI != "" || len(view.Source.Embeds) != 0 {
			return fmt.Errorf("generation view %q requires resolved SQL before current-state derivation", view.CanonicalName())
		}
		sql = "SELECT * FROM " + view.Source.Table
	}
	// Preserve authored WHERE, joins and projections inside the derived table.
	// CompositeIn narrows that read; it must never replace authored row scope.
	var columns []string
	for _, col := range current.Columns {
		if col != nil {
			columns = append(columns, `r."`+strings.ReplaceAll(col.Name, `"`, `""`)+`"`)
		}
	}
	current.Source = view.Source.Clone()
	current.Source.URI = ""
	current.Source.SQL = "SELECT " + strings.Join(columns, ", ") + " FROM (" + strings.TrimSuffix(sql, ";") + ") r WHERE " + predicate

	p := &spec.Parameter{Name: currentName, Source: spec.BindSource{Kind: "view", Name: currentName}, Cardinality: "Many"}
	if err := b.append(p); err != nil {
		return err
	}
	b.request.Component.Views = append(b.request.Component.Views, current)
	currentIdentity, err := current.Identity()
	if err != nil {
		return err
	}
	b.request.ViewBindings[p.Identity()] = currentIdentity
	return nil
}
