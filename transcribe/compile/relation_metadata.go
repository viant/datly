package compile

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
)

// resolveRelationProjections runs after resources resolve and before the plan
// is published. Fields keep their typed output identity, while SQL predicates
// use the corresponding source expression in the named source's scope.
func resolveRelationProjections(root *data.View) error {
	visited := map[*data.View]bool{}
	var visit func(*data.View) error
	visit = func(parent *data.View) error {
		if parent == nil || visited[parent] {
			return nil
		}
		visited[parent] = true
		for _, relation := range parent.Relations {
			if relation == nil || relation.Of == nil || relation.Of.View == nil {
				continue
			}
			if err := resolveProjectedLinks(parent, relation.On, false); err != nil {
				return fmt.Errorf("relation %s parent: %w", relation.Name, err)
			}
			child := relation.Of.View
			if err := resolveProjectedLinks(child, relation.Of.On, true); err != nil {
				return fmt.Errorf("relation %s child: %w", relation.Name, err)
			}
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

// Only a closed FROM/JOIN source can prove that a required key is unavailable.
// A physical source's outer list may be narrowed for an invocation that prunes
// the relation. Do not preempt that selection with an unconditional key check.
// Only the child's predicate side changes scope; Go field identity is retained.
func resolveProjectedLinks(view *data.View, links data.Links, rewrite bool) (err error) {
	// Bootstrap resolves proven aliases and rejects closed-source contradictions.
	// Unresolved SQL belongs to the selected reader build, after template
	// evaluation. In particular, an unselected malformed child must not run.
	defer func() {
		var unresolved *dsql.UnresolvedProjectionError
		if errors.As(err, &unresolved) {
			err = nil
		}
	}()
	for _, link := range links {
		if link != nil && view.Spec.Namespace != "" && link.Namespace != "" && !strings.EqualFold(link.Namespace, view.Spec.Namespace) {
			return fmt.Errorf("namespace %s conflicts with view %s", link.Namespace, view.Spec.Namespace)
		}
	}
	if len(links) == 0 || view.Spec.Source == nil || strings.TrimSpace(view.Spec.Source.SQL) == "" {
		return nil
	}
	source, err := new(builder.Builder).ProjectionSource(view.Spec.Source.SQL)
	if err != nil {
		return err
	}
	projection := dsql.SelectorProjection{SQL: source, View: view}
	columns, err := projection.Columns(nil)
	if err != nil {
		_, complete, probeErr := projection.HasOutput("")
		if probeErr == nil && !complete {
			return nil
		}
		return err
	}
	for _, link := range links {
		if link == nil {
			continue
		}
		matched := false
		sqlColumn := link.Column
		for _, metadata := range view.Columns {
			if metadata == nil || metadata.Column == "" || (!strings.EqualFold(metadata.Name, link.Column) && !strings.EqualFold(metadata.Name, link.Field)) {
				continue
			}
			found, _, err := projection.HasOutput(metadata.Column)
			if err != nil {
				return err
			}
			if found {
				sqlColumn = metadata.Column
				break
			}
		}
		reference := sqlColumn
		if link.Namespace != "" {
			reference = link.Namespace + "." + reference
		}
		for _, column := range columns {
			parts, parseErr := sqlparser.TableIdentifierParts(column.SourceExpression())
			sourceReference := column.SourceExpression()
			if parseErr == nil && len(parts) == 1 && link.Namespace != "" {
				sourceReference = link.Namespace + "." + sourceReference
			}
			if !column.MatchesOutput(sqlColumn) && !(dsql.ProjectionNames{sourceReference}).Matches(reference) {
				continue
			}
			if parseErr != nil || len(parts) == 0 || len(parts) > 2 {
				// Parent collectors consume the projected value itself. Only a
				// child predicate requires a direct source-column expression.
				if !rewrite && column.MatchesOutput(sqlColumn) {
					matched = true
					break
				}
				return fmt.Errorf("matching column %s has no direct SQL source", link.Column)
			}
			namespace, name := "", parts[len(parts)-1]
			if len(parts) == 2 {
				namespace = parts[0]
			}
			found, complete, err := projection.HasSourceOutput(namespace, name)
			if err != nil {
				return err
			}
			if complete && !found {
				return fmt.Errorf("required relation output %s.%s is absent from its source projection", namespace, name)
			}
			link.Output = column.OutputName()
			if rewrite {
				if namespace != "" {
					link.Namespace = namespace
				}
				link.Column = name
			}
			matched = true
			break
		}
		if !matched {
			found, complete, err := projection.HasSourceOutput(link.Namespace, link.Column)
			if err != nil {
				return err
			}
			if complete && !found {
				return fmt.Errorf("required relation output %s.%s is absent from its source projection", link.Namespace, link.Column)
			}
		}
	}
	return nil
}

// BackfillRelationMetadata persists SQL proofs at the authoring boundary. The
// linked loader consumes the resulting on tags without parsing SQL again.
func BackfillRelationMetadata(component *spec.Component, resources ...fs.FS) error {
	if component == nil {
		return nil
	}
	visited := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		runtime := data.FromView(nil, view)
		// Resolve only this edge here; visit owns recursion and shared graphs.
		relationIndex := 0
		for _, relation := range view.Relations {
			if relation == nil {
				continue
			}
			current := runtime.Relations[relationIndex]
			relationIndex++
			if relation.View == nil {
				continue
			}
			linkIndex := 0
			for _, link := range relation.On {
				if link == nil {
					continue
				}
				parent, child := current.On[linkIndex], current.Of.On[linkIndex]
				linkIndex++
				if link.ParentOutput == "" {
					if err := resolveMetadataSQL(runtime, resources); err != nil {
						return err
					}
					if err := resolveProjectedLinks(runtime, data.Links{parent}, false); err != nil {
						return err
					}
					link.ParentOutput = parent.OutputColumn()
				}
				if link.ChildOutput == "" {
					if err := resolveMetadataSQL(current.Of.View, resources); err != nil {
						return err
					}
					if err := resolveProjectedLinks(current.Of.View, data.Links{child}, true); err != nil {
						return err
					}
					link.ChildNamespace, link.ChildColumn, link.ChildOutput = child.Namespace, child.Column, child.OutputColumn()
				}
				if link.ParentField == "" {
					link.ParentField = relationFieldName(view, link.ParentOutput)
				}
				if link.ChildField == "" {
					link.ChildField = relationFieldName(relation.View, link.ChildOutput)
				}
			}
			if err := visit(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := visit(view); err != nil {
			return err
		}
	}
	return nil
}

func relationFieldName(view *spec.View, output string) string {
	for _, column := range view.Columns {
		if column != nil && (strings.EqualFold(column.Source, output) || strings.EqualFold(column.Name, output)) {
			return typecatalog.FieldName(column.Name)
		}
	}
	return typecatalog.FieldName(output)
}

func resolveMetadataSQL(view *data.View, resources []fs.FS) error {
	if view == nil {
		return nil
	}
	view.Spec.Source = view.Spec.RuntimeSource()
	source := view.Spec.Source
	if source == nil || (len(source.Embeds) == 0 && (strings.TrimSpace(source.SQL) != "" || source.URI == "")) {
		return nil
	}
	if len(resources) == 0 || resources[0] == nil {
		return fmt.Errorf("relation/projection metadata for view %s requires SQL resources", view.Spec.Name)
	}
	return dsql.ResolveSource(view.Spec.Name, source, resources[0])
}
