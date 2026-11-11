package reader

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/exec"
	dsql "github.com/viant/datly/sql"
)

// prepareSelectorPresentation resolves exposure names against this compiled
// reader's exact row graph. It never changes shared view metadata.
func (s *Session) prepareSelectorPresentation(ctx context.Context) error {
	s.selectorPresentation = make(map[*data.View][]data.SelectorField)
	visited := map[*data.View]bool{}
	var visit func(*ViewPlan, string) error
	visit = func(plan *ViewPlan, path string) error {
		if plan == nil || plan.View == nil || visited[plan.View] {
			return nil
		}
		visited[plan.View] = true
		if plan.Collector != nil {
			row := plan.Collector.Schema.RowType()
			for row != nil && (row.Kind() == reflect.Ptr || row.Kind() == reflect.Slice) {
				row = row.Elem()
			}
			names, err := exec.PresentedSelectorFields(ctx, s.Component.Key, row, path)
			if err != nil {
				return err
			}
			aliases := map[string]string{}
			for _, name := range names {
				for _, field := range plan.View.SelectorFields {
					if name.GoName != field.GoName || !reflect.DeepEqual(name.Index, field.Index) {
						continue
					}
					for alias, previous := range aliases {
						if (dsql.ProjectionNames{alias}).Matches(name.PublicName) && previous != field.GoName {
							return fmt.Errorf("ambiguous selector presentation field %q", name.PublicName)
						}
					}
					aliases[name.PublicName] = field.GoName
					field.PublicName = name.PublicName
					field.Index = append([]int(nil), field.Index...)
					s.selectorPresentation[plan.View] = append(s.selectorPresentation[plan.View], field)
				}
			}
		}
		for _, edge := range plan.Relations {
			if edge == nil || edge.Relation == nil {
				continue
			}
			childPath := edge.Relation.Holder
			if path != "" && !edge.Relation.IsOutput() {
				childPath = path + "." + childPath
			}
			if err := visit(edge.Target, strings.Trim(childPath, ".")); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(s.Artifact.Root, s.Artifact.OutputViewField)
}

func (s *Session) presentedSelectorName(view *data.View, name string) string {
	for _, field := range s.selectorPresentation[view] {
		if (dsql.ProjectionNames{field.PublicName}).Matches(name) {
			return field.GoName
		}
	}
	return name
}
