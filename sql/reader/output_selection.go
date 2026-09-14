package reader

import (
	"fmt"
	"strings"

	dexec "github.com/viant/datly/exec"
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
				columns = append(columns, name)
			}
		}
		if len(columns) > 0 && plan.Collector != nil {
			matched, err := sqlxio.NewMatcher(nil).Match(plan.Collector.Schema.RowType(), sqlxio.NamesToColumns(columns))
			if err != nil && !sqlxio.IsMatchedError(err) {
				return err
			}
			for _, field := range matched {
				if index := field.FieldIndex(); len(index) > 0 {
					scope.Indexes = append(scope.Indexes, index)
				}
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
