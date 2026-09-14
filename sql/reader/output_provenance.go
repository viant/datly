package reader

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/sql/reader/collector"
	"github.com/viant/datly/sql/reader/readmeta"
)

// outputRead keeps a native query's actual schema with its typed first row.
// It is local to one partition/query, never shared mutable execution state.
type outputRead struct {
	value  reflect.Value
	fields *readmeta.Fields
}

func (e *outputRelationExecution) publish(fields *readmeta.Fields, found bool) error {
	if !e.session.CollectProjection {
		return nil
	}
	if e.session.outputSlots == nil {
		e.session.outputSlots = map[string][]*readmeta.Record{}
	}
	holder := e.planned.Relation.Holder
	if _, exists := e.session.outputSlots[holder]; exists {
		return fmt.Errorf("duplicate output evidence holder %s", holder)
	}
	var rows []*readmeta.Record
	if found {
		rows = []*readmeta.Record{readmeta.NewRecord(fields, nil)}
	}
	e.session.outputSlots[holder] = rows
	return nil
}

func (s *Session) completeProjection(root *collector.Collector) error {
	if !s.CollectProjection {
		return nil
	}
	projection := readmeta.NewResult(s.Artifact.OutputViewField, s.Artifact.DirectOutput, nil)
	var err error
	if root != nil {
		projection, err = root.Provenance(s.Artifact.OutputViewField, s.Artifact.DirectOutput)
		if err != nil {
			return err
		}
	}
	projection, err = projection.WithOutputs(s.outputSlots)
	if err != nil {
		return err
	}
	s.Projection = projection
	return nil
}
