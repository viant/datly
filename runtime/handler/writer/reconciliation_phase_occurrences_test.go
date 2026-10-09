package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
)

type phaseOccurrenceInput struct{ Rows []*phaseTestRoot }

func withPhaseOccurrenceContext(t *testing.T, check func(context.Context)) {
	t.Helper()
	db := sqlite.New(t)
	_, err := engine.New().Execute(context.Background(), engine.Request{
		Input:      protectedEngineRoute(t, reflect.TypeFor[struct{}]()),
		DataSource: dml.Source{DB: db.DB},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			check(ctx)
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func phaseOccurrenceFixture(t *testing.T) (*Program, *finiteSourcePhases, *phaseOccurrenceInput) {
	t.Helper()
	root, declaration := finitePhaseFixture()
	// These fields are canonical marked identity/link fields, without child
	// hooks: preparation must only capture the original graph and Current.
	for _, relation := range root.Relations {
		relation.Child.Keys[0].Has = nil
	}
	phases, err := compileFiniteSourcePhases(root, declaration)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := compileFiniteRootDecision(root)
	if err != nil {
		t.Fatal(err)
	}
	root.reconciliation = &reconciliationMetadata{rootDecision: decision}
	for _, relation := range root.Relations {
		root.reconciliation.roles = append(root.reconciliation.roles, reconciliationRoleMetadata{declaration: spec.ReconciliationRole{Holder: root.EntityType.FieldByIndex(relation.Field).Name}, relation: relation})
	}
	input := &phaseOccurrenceInput{Rows: []*phaseTestRoot{
		{ID: 1, Has: &phaseTestHas{ID: true}, Children: []*phaseTestChild{{ParentID: 1, Value: "new-a"}}},
		{ID: 2, Has: &phaseTestHas{ID: true}, Children: []*phaseTestChild{{ParentID: 2, Value: "new-b"}}},
	}}
	p := &Program{input: input, metadata: &Metadata{Root: root, InputField: 0}, frames: &MutationFrames{}, database: &DatabaseSnapshot{ByRecord: map[*Record][]reflect.Value{}}, previousFields: map[*Record]fieldSet{}}
	for _, relation := range root.Relations {
		p.previousFields[relation.Child] = fieldSet{"ID": true, "ParentID": true, "Value": true}
	}
	// Interleaved source enumeration deliberately differs from root grouping.
	for _, row := range []*phaseTestChild{{ID: 20, ParentID: 2}, {ID: 10, ParentID: 1}, {ID: 21, ParentID: 2}, {ID: 11, ParentID: 1}} {
		p.database.ByRecord[root.Relations[0].Child] = append(p.database.ByRecord[root.Relations[0].Child], reflect.ValueOf(row))
	}
	for i, row := range input.Rows {
		frame := &Frame{Record: root, Entity: reflect.ValueOf(row), Original: detachedOriginal{fieldSet{"ID": true}, true}, holderPosition: i, holderIndexed: true, holderTracked: true}
		p.frames.Rows = append(p.frames.Rows, frame)
		for j, child := range row.Children {
			p.frames.Rows = append(p.frames.Rows, &Frame{Record: root.Relations[0].Child, Entity: reflect.ValueOf(child), Parent: frame, Original: detachedOriginal{fieldSet{"Value": true}, true}, holderPosition: j, holderIndexed: true, holderTracked: true})
		}
	}
	if err = p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
		t.Fatal(err)
	}
	return p, phases, input
}

func TestFinitePhaseOccurrencesBeforeAllocationAndGlobalCurrentOrder(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, phases, input := phaseOccurrenceFixture(t)
		if err := p.prepareFinitePhaseOccurrences(ctx, phases); err != nil {
			t.Fatal(err)
		}
		observations, err := (ReconciliationContext{p.reconciliation}).Roots()
		if err != nil || len(observations) != 2 {
			t.Fatal(observations, err)
		}
		var refs []OccurrenceRef
		for i, root := range observations {
			if root.Allocated != nil || root.Preallocation == nil || root.Roles[0].Working[0].Allocated != nil {
				t.Fatal("allocation was fabricated before native allocation")
			}
			root.Row.(*phaseTestRoot).Children[0].Value = "detached"
			if input.Rows[i].Children[0].Value == "detached" || input.Rows[i].Children[0].ID != 0 {
				t.Fatal("selection observation changed working rows or allocated identity")
			}
			refs = append(refs, p.reconciliation.roots[i].roles[0].current...)
		}
		ordered, err := p.reconciliation.orderedPhaseCurrent(p.metadata.Root.Relations[0].Child, refs)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int
		for _, ref := range ordered {
			ids = append(ids, ref.ticket.previous.Interface().(*phaseTestChild).ID)
		}
		if !reflect.DeepEqual(ids, []int{20, 10, 21, 11}) || reflect.DeepEqual(ordered, refs) {
			t.Fatal("global Current ordinal lost", ids)
		}
		if err = p.prepareFinitePhaseOccurrences(ctx, phases); err == nil {
			t.Fatal("authority recaptured")
		}
	})
}

func TestFinitePhaseOccurrenceFailuresRetireAuthority(t *testing.T) {
	for _, name := range []string{"foreign-compiler", "changed-root", "reordered-root-frames", "duplicate-root", "root-position", "foreign-role", "role-name", "missing-role", "child-pointer", "child-parent", "child-position", "missing-child", "missing-current-field", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, phases, input := phaseOccurrenceFixture(t)
				switch name {
				case "foreign-compiler":
					copy := *phases
					copy.root = &Record{}
					phases = &copy
				case "changed-root":
					input.Rows[1].ID = 99
				case "reordered-root-frames":
					p.frames.Rows[0], p.frames.Rows[2] = p.frames.Rows[2], p.frames.Rows[0]
				case "duplicate-root":
					p.frames.Rows[2] = p.frames.Rows[0]
				case "root-position":
					p.frames.Rows[2].holderPosition = 0
				case "foreign-role":
					copy := *p.metadata.Root.reconciliation.roles[0].relation
					p.metadata.Root.reconciliation.roles[0].relation = &copy
				case "role-name":
					p.metadata.Root.reconciliation.roles[0].declaration.Holder = "Other"
				case "missing-role":
					p.metadata.Root.reconciliation.roles = nil
				case "child-pointer":
					p.frames.Rows[1].Entity = reflect.ValueOf(&phaseTestChild{ParentID: 1})
				case "child-parent":
					p.frames.Rows[1].Parent = p.frames.Rows[2]
				case "child-position":
					p.frames.Rows[1].holderPosition = 1
				case "missing-child":
					p.frames.Rows = append(p.frames.Rows[:1], p.frames.Rows[2:]...)
				case "missing-current-field":
					delete(p.previousFields[p.metadata.Root.Relations[0].Child], "ParentID")
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelled
				}
				if err := p.prepareFinitePhaseOccurrences(ctx, phases); err == nil {
					t.Fatal("invalid authority captured")
				}
				if p.reconciliation != nil && p.reconciliation.active {
					t.Fatal("failed preparation left active tickets")
				}
			})
		})
	}
}

func TestFinitePhaseCurrentOrderPreservesOccurrencesAndRejectsForeignRefs(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, phases, _ := phaseOccurrenceFixture(t)
		if err := p.prepareFinitePhaseOccurrences(ctx, phases); err != nil {
			t.Fatal(err)
		}
		a := p.reconciliation
		ref := a.roots[0].roles[0].current[0]
		ordered, err := a.orderedPhaseCurrent(ref.ticket.record, []OccurrenceRef{ref, ref})
		if err != nil || len(ordered) != 2 {
			t.Fatal("source occurrences deduplicated", err)
		}
		for _, invalid := range []OccurrenceRef{{}, a.roots[0].ref, {ticket: &reconciliationTicket{owner: a, current: true, currentOrdinal: 0, record: ref.ticket.record}}} {
			if _, err = a.orderedPhaseCurrent(ref.ticket.record, []OccurrenceRef{invalid}); err == nil {
				t.Fatal("forged or working occurrence used as bound Current")
			}
		}
		a.active = false
		if _, err = a.orderedPhaseCurrent(ref.ticket.record, []OccurrenceRef{ref}); err == nil {
			t.Fatal("retired occurrence accepted")
		}
		if _, err = a.orderedPhaseCurrent(ref.ticket.record, nil); err == nil {
			t.Fatal("retired empty selection accepted")
		}
		if _, err = (*reconciliationAttempt)(nil).orderedPhaseCurrent(ref.ticket.record, nil); err == nil {
			t.Fatal("nil empty selection accepted")
		}
	})
}
