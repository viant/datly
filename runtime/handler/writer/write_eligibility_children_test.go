package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"

	h "github.com/viant/xdatly/handler"
)

func eligibilityChildrenFixture(t *testing.T) (*Program, *Frame) {
	p, _ := eligibilityFixture(t)
	root := p.metadata.Root
	child := &Record{EntityType: reflect.TypeFor[eligibleRow](), Table: "child"}
	root.Relations = []*Relation{{Child: child, Links: []Link{{Parent: root.Fields[0], Child: root.Fields[0]}}}}
	if err := validateWriteEligibilityHooks(p.metadata, reflect.TypeFor[eligibleOutput]()); err != nil {
		t.Fatal(err)
	}
	frame := p.frames.Rows[0]
	frame.Action = h.WriteUpdate
	frame.Previous = reflect.ValueOf(&eligibleRow{ID: 1})
	p.actions.Rows[0].Kind = h.WriteUpdate
	p.previousFields = map[*Record]fieldSet{root: {"ID": true}}
	childFrame := &Frame{Record: child, Entity: reflect.ValueOf(&eligibleRow{ID: 3}), Action: h.WriteInsert, Parent: frame}
	p.frames.Rows = append(p.frames.Rows, childFrame)
	action := &Action{Kind: h.WriteInsert, Entity: childFrame.Entity, frame: childFrame}
	p.actions.Rows = append(p.actions.Rows, action)
	p.queueItems = append(p.queueItems, action)
	return p, frame
}

func TestWriteEligibilityKeepsDirectChildActions(t *testing.T) {
	p, parent := eligibilityChildrenFixture(t)
	child := p.actions.Rows[2]
	if err := p.decideWriteEligibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.filterIneligibleActions()
	if !parent.ExcludedWrite || len(p.actions.Rows) != 2 || p.actions.Rows[1] != child || len(p.frames.Rows) != 3 {
		t.Fatal("root suppression removed child or source frame")
	}
	if len(p.input.(*eligibleInput).Rows) != 2 {
		t.Fatal("root suppression removed response body")
	}
}

func TestWriteEligibilityExcludedParentRequiresPersistedProducer(t *testing.T) {
	for _, mode := range []string{"insert", "missing previous", "partial previous", "changed key"} {
		t.Run(mode, func(t *testing.T) {
			p, parent := eligibilityChildrenFixture(t)
			switch mode {
			case "insert":
				parent.Action = h.WriteInsert
			case "missing previous":
				parent.Previous = reflect.Value{}
			case "partial previous":
				delete(p.previousFields[parent.Record], "ID")
			case "changed key":
				parent.Previous = reflect.ValueOf(&eligibleRow{ID: 77})
			}
			if err := p.decideWriteEligibility(context.Background()); err == nil || !strings.Contains(err.Error(), "WriteEligible") {
				t.Fatalf("unsafe exclusion admitted: %v", err)
			}
			if parent.ExcludedWrite {
				t.Fatal("failed exclusion changed membership")
			}
		})
	}
}

func TestWriteEligibilityRejectsDeeperWritableGraphs(t *testing.T) {
	p, _ := eligibilityChildrenFixture(t)
	child := p.metadata.Root.Relations[0].Child
	child.Relations = []*Relation{{Child: &Record{Table: "grandchild"}}}
	if err := validateWriteEligibilityHooks(p.metadata, reflect.TypeFor[eligibleOutput]()); err == nil {
		t.Fatal("unreviewed nested writable graph admitted")
	}
}
