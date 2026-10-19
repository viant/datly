package collector

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestProvenanceConcurrentSiblingResetAndAttach(t *testing.T) {
	type row struct{ ID int }
	rows := make([]row, 32)
	parent := NewCollector(&View{View: &data.View{}, Schema: NewSchema(reflect.TypeOf(row{}))}, &rows, true)
	parent.provenance = true
	const siblings = 4
	children := make([]*Collector, siblings)
	for i := range children {
		children[i] = &Collector{parent: parent, provenance: true, relation: &Relation{Relation: &data.Relation{Holder: fmt.Sprintf("child%d", i), Cardinality: spec.CardinalityMany}}}
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, child := range children {
		workers.Add(1)
		go func(child *Collector) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 100; iteration++ {
				child.resetEvidenceHolder()
				for position := range rows {
					child.attachEvidence(position, 0)
				}
			}
		}(child)
	}
	close(start)
	workers.Wait()
	if len(parent.rowEvidence) != len(rows) {
		t.Fatalf("parent evidence rows: got %d, want %d", len(parent.rowEvidence), len(rows))
	}
	for position, evidence := range parent.rowEvidence {
		if len(evidence.relations) != siblings {
			t.Fatalf("row %d: got %d holders, want %d", position, len(evidence.relations), siblings)
		}
		for _, child := range children {
			edges := evidence.relations[child.relation.Holder]
			if len(edges) != 1 || edges[0] != child.rowEvidence[0] {
				t.Fatalf("row %d holder %s: final attachment was lost or duplicated", position, child.relation.Holder)
			}
		}
	}
	if _, err := parent.Provenance("Data", false); err != nil {
		t.Fatal(err)
	}
}
