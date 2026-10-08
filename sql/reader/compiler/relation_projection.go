package compiler

import (
	"fmt"
	"github.com/viant/datly/data"
	"strings"
)

// resolveRelationProjections binds linked metadata without inspecting SQL.
// SQL source/output proofs belong to transcription. The on tag owns predicate
// scope; reflected SQLX columns own result labels unless explicitly supplied.
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
			if err := resolveMetadataLinks(parent, relation.On); err != nil {
				return fmt.Errorf("relation %s parent: %w", relation.Name, err)
			}
			if err := resolveMetadataLinks(relation.Of.View, relation.Of.On); err != nil {
				return fmt.Errorf("relation %s child: %w", relation.Name, err)
			}
			if err := visit(relation.Of.View); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

func resolveMetadataLinks(view *data.View, links data.Links) error {
	for _, link := range links {
		if link == nil {
			return fmt.Errorf("relation link is required")
		}
		if view.Spec.Namespace != "" && link.Namespace != "" && !strings.EqualFold(view.Spec.Namespace, link.Namespace) {
			return fmt.Errorf("namespace %s conflicts with view %s", link.Namespace, view.Spec.Namespace)
		}
		if strings.TrimSpace(link.Column) == "" {
			return fmt.Errorf("relation SQL column is required")
		}
		var matched *data.Column
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			if (link.Field != "" && strings.EqualFold(column.Name, link.Field)) || (link.Field == "" && (strings.EqualFold(column.Column, link.Column) || strings.EqualFold(column.Name, link.Column))) {
				if matched != nil {
					return fmt.Errorf("ambiguous relation field %q column %q", link.Field, link.Column)
				}
				matched = column
			}
		}
		if matched != nil {
			if link.Field == "" {
				link.Field = matched.Name
			}
			if link.Output == "" {
				link.Output = matched.Output
				if link.Output == "" {
					link.Output = matched.Column
					if matched.Column == matched.Name {
						link.Output = link.Column
					}
				}
			}

		}
		if link.Output == "" {
			link.Output = link.Column
		}
	}
	return nil
}
