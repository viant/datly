package compiler

import (
	"errors"
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
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
		reference := link.Column
		if link.Namespace != "" {
			reference = link.Namespace + "." + reference
		}
		for _, column := range columns {
			parts, parseErr := sqlparser.TableIdentifierParts(column.SourceExpression())
			sourceReference := column.SourceExpression()
			if parseErr == nil && len(parts) == 1 && link.Namespace != "" {
				sourceReference = link.Namespace + "." + sourceReference
			}
			if !column.MatchesOutput(link.Column) && !(dsql.ProjectionNames{sourceReference}).Matches(reference) {
				continue
			}
			if parseErr != nil || len(parts) == 0 || len(parts) > 2 {
				// Parent collectors consume the projected value itself. Only a
				// child predicate requires a direct source-column expression.
				if !rewrite && column.MatchesOutput(link.Column) {
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
