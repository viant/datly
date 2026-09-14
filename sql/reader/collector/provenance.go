package collector

import (
	"fmt"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/readmeta"
)

type rowEvidence struct {
	source    *data.View
	fields    *readmeta.Fields
	relations map[string][]*rowEvidence
}

// EnableProvenance opts this collector tree into final-ordinal read evidence.
// It must be called before scanning; normal reads allocate no evidence graph.
func (r *Collector) EnableProvenance() error {
	if err := r.checkProvenanceStart(); err != nil {
		return err
	}
	r.enableProvenance()
	return nil
}

func (r *Collector) checkProvenanceStart() error {
	if r == nil {
		return fmt.Errorf("collector is required")
	}
	if r.Len() != 0 || r.indexCounter != 0 {
		return fmt.Errorf("provenance must be enabled before scanning")
	}
	for _, child := range r.relations {
		if err := child.checkProvenanceStart(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Collector) enableProvenance() {
	r.provenance = true
	for _, child := range r.relations {
		child.enableProvenance()
	}
}

func (r *Collector) evidence(position int) *rowEvidence {
	if !r.provenance {
		return nil
	}
	for len(r.rowEvidence) <= position {
		r.rowEvidence = append(r.rowEvidence, &rowEvidence{source: r.View(), relations: map[string][]*rowEvidence{}})
	}
	return r.rowEvidence[position]
}

// RecordFields records the schema for the next accepted visitor row. Allocation
// addresses are deliberately not retained: value slices may grow and move.
func (r *Collector) RecordFields(fields *readmeta.Fields) {
	if r != nil && r.provenance {
		r.evidence(r.indexCounter).fields = fields
	}
}

func (r *Collector) attachEvidence(parentPosition, childPosition int) {
	if !r.provenance || r.parent == nil || !r.parent.provenance {
		return
	}
	r.Lock().Lock()
	defer r.Lock().Unlock()
	parent, child := r.parent.evidence(parentPosition), r.evidence(childPosition)
	holder := r.relation.Holder
	if r.relation.Cardinality == spec.CardinalityOne {
		parent.relations[holder] = []*rowEvidence{child}
		return
	}
	parent.relations[holder] = append(parent.relations[holder], child)
}

func (r *Collector) resetEvidenceHolder() {
	if !r.provenance || r.parent == nil || !r.parent.provenance {
		return
	}
	for i := 0; i < r.parent.Len(); i++ {
		r.parent.evidence(i).relations[r.relation.Holder] = nil
	}
}

// Provenance snapshots the completed collector graph. It never walks values or
// re-matches relationships; edges came from actual attachment operations.
func (r *Collector) Provenance(rootHolder string, direct bool) (*readmeta.Result, error) {
	if r == nil || !r.provenance {
		return nil, nil
	}
	if len(r.rowEvidence) != r.Len() {
		return nil, fmt.Errorf("collector evidence count %d differs from final rows %d", len(r.rowEvidence), r.Len())
	}
	state := &evidenceSnapshot{done: map[*rowEvidence]*readmeta.Record{}, active: map[*rowEvidence]bool{}}
	rows, err := state.records(r.rowEvidence)
	if err != nil {
		return nil, err
	}
	return readmeta.NewResult(rootHolder, direct, rows), nil
}

type evidenceSnapshot struct {
	done   map[*rowEvidence]*readmeta.Record
	active map[*rowEvidence]bool
}

func (s *evidenceSnapshot) records(source []*rowEvidence) ([]*readmeta.Record, error) {
	result := make([]*readmeta.Record, len(source))
	for i, row := range source {
		if row == nil {
			result[i] = readmeta.NewRecord(nil, nil)
			continue
		}
		if s.active[row] {
			return nil, fmt.Errorf("read evidence contains a cycle")
		}
		if prior := s.done[row]; prior != nil {
			result[i] = prior
			continue
		}
		s.active[row] = true
		relations := make(map[string][]*readmeta.Record, len(row.relations))
		for holder, children := range row.relations {
			var err error
			relations[holder], err = s.records(children)
			if err != nil {
				return nil, err
			}
		}
		delete(s.active, row)
		result[i] = readmeta.NewRecord(row.fields, relations)
		s.done[row] = result[i]
	}
	return result, nil
}
