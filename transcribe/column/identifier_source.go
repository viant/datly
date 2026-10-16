package column

import (
	"fmt"
	"io/fs"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/viant/datly/constant/identifier"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

type identifierOrigin struct {
	view                          *spec.View
	connector, authored, resolved string
	dynamic                       bool
}

// identifierAnalysis assigns source-position markers, never discovery values.
// Replacing these markers restores exact authored references even when several
// constants, or an explicit literal, have the same configured value.
func identifierAnalysis(SQL string, input *TemplateInput) (string, *strings.Replacer, error) {
	renderer := identifier.Renderer{}
	if input != nil {
		renderer.Values = input.Const
		for _, variable := range input.Variables {
			renderer.Variables = append(renderer.Variables, variable.Name)
		}
	}
	refs, err := renderer.References(SQL)
	if err != nil {
		return "", nil, err
	}
	_, spans, err := veltyparser.ParseWithSpansDetailed([]byte(SQL))
	if err != nil {
		return "", nil, err
	}
	for value, span := range spans {
		selector, ok := value.(*expr.Select)
		if !ok || selector.ID != "Unsafe" {
			continue
		}
		start, _, kind := sqltext.ProtectedRangeAt(SQL, span.Start)
		if kind != "" && SQL[start] != '`' && SQL[start] != '[' {
			continue
		}
		member, ok := selector.X.(*expr.Select)
		if !ok || member.X != nil {
			continue // Calls and arbitrary fragments cannot establish table identity.
		}
		refs = append(refs, identifier.Reference{Start: span.Start, End: span.End + 1})
	}
	prefix := "__datly_identifier_"
	for strings.Contains(SQL, prefix) {
		prefix = "_" + prefix
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Start > refs[j].Start })
	analysis := SQL
	var pairs []string
	for i, ref := range refs {
		if ref.Start < 0 || ref.End > len(SQL) || ref.Start >= ref.End {
			return "", nil, fmt.Errorf("invalid authored identifier span")
		}
		marker := fmt.Sprintf("%s%d__", prefix, i)
		pairs = append(pairs, marker, SQL[ref.Start:ref.End])
		analysis = analysis[:ref.Start] + marker + analysis[ref.End:]
	}
	return analysis, strings.NewReplacer(pairs...), nil
}

func authoredDiscoveryTable(source *spec.ViewSource, input *TemplateInput, auxiliary bool) (string, error) {
	analysis, restore, err := identifierAnalysis(source.SQL, input)
	if err != nil {
		return "", err
	}
	table := directSourceTable(analysis)
	if auxiliary {
		table = explicitAuxiliarySource(analysis)
	}
	return restore.Replace(table), nil
}

func (r *Compilation) captureIdentifierOrigins(component *spec.Component, resources fs.FS, input *TemplateInput) error {
	if err := normalizeIdentifierMetadata(component, input); err != nil {
		return err
	}
	bindings := newIdentifierBindings(component, input)
	if r.identifiers == nil {
		r.identifiers = map[*spec.View]identifierOrigin{}
	}
	var visit func(*spec.View, string) error
	visit = func(view *spec.View, connector string) error {
		if view == nil {
			return nil
		}
		if _, seen := r.identifiers[view]; seen {
			return nil
		}
		origin := identifierOrigin{view: view, connector: connector}
		r.identifiers[view] = origin
		if view.Source != nil {
			source := view.Source.Clone()
			if source.Bindings != nil && source.Bindings.Connector != "" {
				origin.connector = source.Bindings.Connector
			}
			assets := resources
			if input != nil {
				assets = input.Const.Resources(assets)
			}
			if err := dsql.ResolveSource(view.Name, source, assets); err != nil {
				return err
			}
			origin.authored = source.Table
			if origin.authored == "" {
				// Discovery will report unsupported/dynamic source shapes itself.
				origin.authored, _ = authoredDiscoveryTable(source, input, false)
			}
			primary, _ := authoredDiscoveryTable(source, input, false)
			if primary == "" {
				primary = origin.authored
			}
			var err error
			origin.dynamic, err = bindings.dynamic(primary)
			if err != nil {
				return fmt.Errorf("view %s source identifier: %w", view.CanonicalName(), err)
			}
			renderer := identifier.Renderer{}
			if input != nil {
				renderer.Values = input.Const
			}
			origin.resolved, _ = renderer.Metadata(origin.authored)
			r.identifiers[view] = origin
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View, origin.connector); err != nil {
					return err
				}
			}
		}
		return nil
	}
	connector := ""
	if component.Settings != nil {
		connector = component.Settings.DefaultConnector
	}
	if err := visit(component.RootView, connector); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := visit(view, connector); err != nil {
			return err
		}
	}
	return nil
}

func (r *Compilation) preserveConstraintTargets(view *spec.View, constraints map[string]tableConstraint, lineage *projectionLineage, product string) (map[string]tableConstraint, error) {
	owner := r.identifiers[view]
	result := make(map[string]tableConstraint, len(constraints))
	for name, constraint := range constraints {
		if constraint.reference == nil {
			result[name] = constraint
			continue
		}
		physicalRef := *constraint.reference
		ref := physicalRef
		constraint.reference = &ref
		var candidates []identifierOrigin
		for other, origin := range r.identifiers {
			if other == view || origin.dynamic || origin.connector != owner.connector || !strings.Contains(origin.authored, "$") || !referenceMatchesOrigin(physicalRef, origin, product) {
				continue
			}
			candidates = append(candidates, origin)
		}
		if len(candidates) > 0 {
			var explicit bool
			for _, column := range view.Columns {
				if column == nil {
					continue
				}
				metadata := sqlio.ParseTag(reflect.StructTag(column.Tag))
				physical := normalizedName(firstValue(column.Source, column.Name))
				if lineage != nil {
					if source, found := lineage.direct[physical]; found {
						physical = source
					}
				}
				if physical == name && metadata.RefTable != "" {
					explicit = true
				}
			}
			if !explicit {
				var target string
				for parent, parentOrigin := range r.identifiers {
					for _, relation := range parent.Relations {
						if relation == nil || relation.View == nil {
							continue
						}
						for _, link := range relation.On {
							if link == nil {
								continue
							}
							var origin identifierOrigin
							if parent == view && normalizedName(firstValue(link.ParentColumn, link.ParentOutput, link.ParentField)) == name && normalizedName(firstValue(link.ChildColumn, link.ChildOutput, link.ChildField)) == normalizedName(ref.column) {
								origin = r.identifiers[relation.View]
							} else if relation.View == view && normalizedName(firstValue(link.ChildColumn, link.ChildOutput, link.ChildField)) == name && normalizedName(firstValue(link.ParentColumn, link.ParentOutput, link.ParentField)) == normalizedName(ref.column) {
								origin = parentOrigin
							}
							for _, candidate := range candidates {
								if origin.view == candidate.view {
									if target != "" && target != candidate.authored {
										return nil, fmt.Errorf("column %s has ambiguous environment-dependent foreign-key target; declare explicit sqlx refTable/refDb metadata", name)
									}
									target = candidate.authored
								}
							}
						}
					}
				}
				if target == "" {
					return nil, fmt.Errorf("column %s has no authored provenance for environment-dependent foreign-key target; declare explicit sqlx refTable/refDb metadata", name)
				}
				ref.table, ref.schema = target, ""
			}
		} else if referenceMatchesOrigin(physicalRef, owner, product) && strings.Contains(owner.authored, "$") {
			// Self-reference is sufficient only without competing symbolic
			// targets. Otherwise an authored relation must establish identity.
			ref.table, ref.schema = owner.authored, ""
		}
		result[name] = constraint
	}
	return result, nil
}

func referenceMatchesOrigin(ref tableReference, origin identifierOrigin, product string) bool {
	parser := sqlparser.TableIdentifierParser{Product: product}
	parts, err := parser.Parts(origin.resolved)
	if err != nil || len(parts) == 0 {
		return false
	}
	target, err := parser.Parts(ref.table)
	if err != nil || len(target) == 0 {
		return false
	}
	if ref.schema == "" {
		return slices.Equal(parts, target)
	}
	if len(parts) < 2 || ref.schema != parts[len(parts)-2] {
		return false
	}
	return slices.Equal(parts, target) || len(target) == 1 && target[0] == parts[len(parts)-1]
}
