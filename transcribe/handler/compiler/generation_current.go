package compiler

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
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
	outputs := currentSourceOutputs(view.Source.SQL)
	var projection []string
	var aliased []string
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
		source := key.Field
		if output := outputs[strings.ToLower(strings.TrimSpace(key.Source))]; output != "" {
			alias = typecatalog.FieldName(output)
			source = alias
			if !strings.EqualFold(strings.TrimSpace(output), strings.TrimSpace(key.Source)) {
				aliased = append(aliased, alias)
			}
		}
		projection = append(projection, source+" AS "+alias)
	}
	query := "SELECT " + strings.Join(projection, ", ") + " FROM `/" + strings.Join(path, "/") + "`"
	parameter := &spec.Parameter{Name: keyName, Source: spec.BindSource{Kind: "param", Name: body}, Cardinality: "Many", DeclarationSQL: query, Codec: &spec.Codec{Body: "structql", Args: []string{query}}}
	if len(aliased) > 0 {
		parameter.Tag = `compositeAlias:"` + strings.Join(aliased, ",") + `"`
	}
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
		// Current-key helpers are generated from the current outer projection.
		// Refresh their declaration when that projection renames a key; retaining
		// the bootstrapped prior SQL would keep the stale helper field and make
		// CompositeIn address the old derived-table column.
		existing.DeclarationSQL = parameter.DeclarationSQL
		existing.Codec = parameter.Codec
		existing.Tag = parameter.Tag
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
	outputs := currentSourceOutputs(sql)
	var columns []string
	for _, col := range current.Columns {
		if col != nil {
			name := col.Name
			for _, candidate := range []string{col.Name, col.Source} {
				if output := outputs[strings.ToLower(strings.TrimSpace(candidate))]; output != "" {
					name = output
					break
				}
			}
			// The derived table and CompositeIn criteria share this canonical
			// current-view column name. Keep Source as the physical entity link
			// so currentProjection can still map the alias back to the entity.
			if !strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(col.Name)) {
				col.Expression = col.Source
				col.Source = name
			}
			col.Name = name
			// The entity may keep a derived relation key transient so it never
			// becomes a DML column. Its generated Current carrier is a read shape,
			// however, and must scan the projected alias for matching/linking.
			col.Tag = currentReadTag(col.Tag, name)
			columns = append(columns, `r."`+strings.ReplaceAll(name, `"`, `""`)+`"`)
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

func currentReadTag(tag, column string) string {
	raw := reflect.StructTag(strings.TrimSpace(tag)).Get("sqlx")
	if raw == "" {
		return tag
	}
	parts := strings.Split(raw, ",")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) != "-" {
		return tag
	}
	parts[0] = strings.TrimSpace(column)
	return strings.Replace(tag, `sqlx:`+strconv.Quote(raw), `sqlx:`+strconv.Quote(strings.Join(parts, ",")), 1)
}

// currentSourceOutputs maps a direct source column to the output name of the
// already-compiled view SQL. The current-state reader wraps that SQL as a
// derived table, so an outer alias (ID AS RootKey) is the only addressable
// name; the physical source name no longer exists at that boundary.
func currentSourceOutputs(SQL string) map[string]string {
	result := map[string]string{}
	SQL = strings.TrimSpace(SQL)
	parsed, err := sqlparser.ParseQuery(SQL)
	if err != nil || parsed == nil || len(parsed.List) == 0 {
		// Generated read programs may have a Velty prelude. The view SELECT is
		// still authoritative and is the first SQL statement in that program.
		if index := strings.Index(strings.ToUpper(SQL), "SELECT "); index >= 0 {
			parsed, err = sqlparser.ParseQuery(SQL[index:])
		}
	}
	if err != nil || parsed == nil {
		return result
	}
	for _, item := range parsed.List {
		column := sqlparser.NewColumn(item)
		if column.Expression != "" || strings.TrimSpace(column.Name) == "" {
			continue
		}
		output := strings.TrimSpace(column.Identity())
		if output == "" {
			output = strings.TrimSpace(column.Name)
		}
		result[strings.ToLower(strings.TrimSpace(column.Name))] = output
		result[strings.ToLower(output)] = output
	}
	return result
}
