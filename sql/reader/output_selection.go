package reader

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	sqlxio "github.com/viant/sqlx/io"
	structjson "github.com/viant/structology/encoding/json"
)

// outputSelection traverses graph edges, not row values. Shared target views
// therefore get a mask at each holder path without collecting per-row evidence.
type outputSelection struct {
	selectors invocationSelectors
	scopes    []structjson.FieldSelection
}

func (s *Session) selectedOutput(selectors invocationSelectors) (dexec.OutputFieldFilter, error) {
	if len(selectors) == 0 {
		return nil, nil
	}
	selection := outputSelection{selectors: selectors}
	var path []string
	if !s.Artifact.DirectOutput {
		path = []string{s.Artifact.OutputViewField}
	}
	if err := selection.view(s.Artifact.Root, path); err != nil {
		return nil, err
	}
	if len(selection.scopes) == 0 {
		return nil, nil
	}
	return structjson.NewFieldFilter(s.OutputType, selection.scopes)
}
func (s *outputSelection) view(plan *ViewPlan, path []string) error {
	if plan == nil || plan.View == nil {
		return fmt.Errorf("output selection requires a view plan")
	}
	requested := projectionSelection(s.selectors.forView(plan.View))
	include := map[string]bool{}
	if len(requested) > 0 {
		scope := structjson.FieldSelection{Path: append([]string(nil), path...)}
		var columns []string
		for _, name := range requested {
			relation := false
			for _, edge := range plan.Relations {
				if strings.EqualFold(name, edge.Relation.Holder) {
					include[edge.Relation.Holder] = true
					scope.Fields = append(scope.Fields, edge.Relation.Holder)
					relation = true
				}
			}
			if !relation {
				column, err := outputSelectionColumn(plan.View, name)
				if err != nil {
					return err
				}
				columns = append(columns, column)
			}
		}
		if len(columns) > 0 && plan.Collector != nil {
			matched, err := sqlxio.NewMatcher(nil).Match(plan.Collector.Schema.RowType(), sqlxio.NamesToColumns(columns))
			if err != nil && !sqlxio.IsMatchedError(err) {
				return err
			}
			matchedIndexes := 0
			for _, field := range matched {
				if index := field.FieldIndex(); len(index) > 0 {
					matchedIndexes++
					scope.Indexes = append(scope.Indexes, index)
				}
			}
			if matchedIndexes != len(columns) {
				return fmt.Errorf("output selection for view %s matched %d of %d requested fields", plan.View.Spec.Name, matchedIndexes, len(columns))
			}
		}
		s.scopes = append(s.scopes, scope)
	}
	for _, edge := range plan.Relations {
		if edge.Relation.IsOutput() {
			if err := s.view(edge.Target, strings.Split(edge.Relation.Holder, ".")); err != nil {
				return err
			}
			continue
		}
		if len(requested) > 0 && !include[edge.Relation.Holder] {
			continue
		}
		child := append(append([]string(nil), path...), strings.Split(edge.Relation.Holder, ".")...)
		if err := s.view(edge.Target, child); err != nil {
			return err
		}
	}
	return nil
}

// SQL selection has already validated the request. Translate its explicit
// canonical aliases back to SQLX field identities, not JSON names or inferred
// Go aliases. This also keeps codec destinations on their model field indexes.
func outputSelectionColumn(view *data.View, name string) (string, error) {
	source := ""
	for _, mapping := range view.Spec.Columns {
		if mapping == nil || mapping.NameInferred || mapping.Source == "" || !(dsql.ProjectionNames{mapping.Name}).Matches(name) {
			continue
		}
		if source != "" && !(dsql.ProjectionNames{source}).Matches(mapping.Source) {
			return "", fmt.Errorf("output selection for view %s has ambiguous alias %q", view.Spec.Name, name)
		}
		source = mapping.Source
	}
	if source != "" {
		name = source
	}
	return outputSelectionColumnName(name), nil
}

func outputSelectionColumnName(name string) string {
	parts, err := sqlparser.TableIdentifierParts(strings.TrimSpace(name))
	if err == nil && len(parts) == 1 {
		return parts[0]
	}
	return name
}
