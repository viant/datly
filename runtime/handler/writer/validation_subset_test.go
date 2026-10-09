package writer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	h "github.com/viant/xdatly/handler"
)

type subsetValidationProbe struct {
	rows       []any
	options    [][]h.ValidationOptions
	failure    error
	violations bool
}

func (v *subsetValidationProbe) Validate(_ context.Context, rows any, options ...any) (*h.Validation, error) {
	v.rows = append(v.rows, rows)
	v.options = append(v.options, options[0].([]h.ValidationOptions))
	if v.failure != nil {
		return nil, v.failure
	}
	if v.violations {
		code := 409
		if len(v.rows) > 1 {
			code = 422
		}
		return &h.Validation{Code: code, Violations: []*h.Violation{{Message: v.options[len(v.options)-1][0].Location}}}, nil
	}
	return &h.Validation{}, nil
}

func subsetValidationFixture(t *testing.T) *Program {
	t.Helper()
	p, _, _ := phaseOccurrenceFixture(t)
	for i, frame := range p.frames.Rows {
		frame.Action = h.WriteInsert
		frame.Location = string(rune('a' + i))
		frame.Fields = livePresence(frame.Record, frame.Entity.Elem())
	}
	return p
}

func TestValidationSubsetRetainsGraphAndTypedGroupOrder(t *testing.T) {
	p := subsetValidationFixture(t)
	graph := p.frames
	before := append([]*Frame(nil), graph.Rows...)
	probe := &subsetValidationProbe{}
	// Interleave roles, reverse child selection, and omit the first parent.
	selected := []*Frame{graph.Rows[3], graph.Rows[2], graph.Rows[1]}
	if err := p.validateFrameSubset(context.Background(), probe, false, selected); err != nil {
		t.Fatal(err)
	}
	children, ok := probe.rows[0].([]*phaseTestChild)
	if !ok || len(children) != 2 || children[0] != graph.Rows[3].Entity.Interface() || children[1] != graph.Rows[1].Entity.Interface() {
		t.Fatal("selected typed child order lost")
	}
	roots, ok := probe.rows[1].([]*phaseTestRoot)
	if !ok || len(roots) != 1 || roots[0] != graph.Rows[2].Entity.Interface() {
		t.Fatal("first occurrence record grouping lost")
	}
	if p.frames != graph || !reflect.DeepEqual(graph.Rows, before) {
		t.Fatal("canonical graph replaced or reordered")
	}
	if err := p.validateFrames(context.Background(), &subsetValidationProbe{}, false); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(graph.Rows, before) {
		t.Fatal("ordinary delegation changed graph")
	}
}

func TestValidationSubsetUsesCanonicalParentReference(t *testing.T) {
	p := subsetValidationFixture(t)
	parent, child := p.frames.Rows[0], p.frames.Rows[1]
	link := &parent.Record.Relations[0].Links[0]
	link.Child.RefTable, link.Child.RefColumn = parent.Record.Table, link.Parent.Column
	probe := &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, true, []*Frame{child}); err != nil {
		t.Fatal(err)
	}
	options := probe.options[0][0]
	if options.Action != h.WriteInsert || options.Location != child.Location || len(options.SatisfiedReferences) == 0 {
		t.Fatal("omitted producer lost canonical parent reference", options)
	}
	child.NoopMissingIdentity = true
	probe = &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{child}); err != nil {
		t.Fatal(err)
	}
	if len(probe.rows) != 1 || probe.options[0][0].Action != h.WriteInsert {
		t.Fatal("no-op candidate incorrectly skipped")
	}
}

func TestValidationSubsetFailureAndAggregation(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		p := subsetValidationFixture(t)
		if aggregate {
			p.hook = reflect.ValueOf(&aggregateValidationProbe{})
		}
		probe := &subsetValidationProbe{violations: true}
		err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{p.frames.Rows[0], p.frames.Rows[1]})
		if err == nil {
			t.Fatal("validation failure lost")
		}
		if aggregate {
			var schema *initialSchemaViolations
			if !errors.As(err, &schema) || len(schema.validation.Violations) != 2 || schema.validation.Code != 409 || schema.validation.Violations[0].Message != "a" || schema.validation.Violations[1].Message != "b" {
				t.Fatal("aggregate failure order or first code lost", err)
			}
		} else if len(probe.rows) != 1 {
			t.Fatal("immediate failure continued")
		}
		operational := errors.New("validator transport failure")
		probe = &subsetValidationProbe{failure: operational}
		if err = p.validateFrameSubset(context.Background(), probe, false, []*Frame{p.frames.Rows[0], p.frames.Rows[1]}); !errors.Is(err, operational) || len(probe.rows) != 1 {
			t.Fatal("operational validator failure continued", err)
		}
	}
}

func TestValidationSubsetPreservesSparseAndTransactionalOptions(t *testing.T) {
	p := subsetValidationFixture(t)
	child := p.frames.Rows[1]
	child.Action = h.WriteUpdate
	previous := &phaseTestChild{ID: 10, ParentID: 1, Value: "previous"}
	child.Previous = reflect.ValueOf(previous)
	probe := &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{child}); err != nil {
		t.Fatal(err)
	}
	options := probe.options[0][0]
	if options.Previous != previous || options.Fields != child.Fields || options.PreviousFields == nil || options.Action != h.WriteUpdate {
		t.Fatal("sparse Previous evidence lost", options)
	}
	child.Action = h.WriteDelete
	probe = &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{child}); err != nil || len(probe.rows) != 0 {
		t.Fatal("initial delete filter changed", err)
	}
	if err := p.validateFrameSubset(context.Background(), probe, true, []*Frame{child}); err != nil {
		t.Fatal(err)
	}
	if len(probe.rows) != 1 || probe.options[0][0].Action != h.WriteUpdate {
		t.Fatal("transactional reconciliation delete validation lost")
	}
}

func TestValidationSubsetPreservesExistingFilters(t *testing.T) {
	p := subsetValidationFixture(t)
	root, child := p.frames.Rows[0], p.frames.Rows[1]
	root.Action, child.Action = h.WriteDelete, h.WriteUpdate
	child.Fields = &presence{record: child.Record, snapshot: make([]uint64, words(len(child.Record.Fields)))}
	root.Record.reconciliation = nil
	probe := &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{nil, {}, root, child}); err != nil {
		t.Fatal(err)
	}
	if len(probe.rows) != 0 {
		t.Fatal("delete or identity-only update filter lost")
	}
	child.Record.writeEligibility = true
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{child}); err != nil {
		t.Fatal(err)
	}
	if len(probe.rows) != 1 || probe.options[0][0].Action != h.WriteUpdate {
		t.Fatal("write eligibility validation skipped")
	}
	child.Record.Auxiliary = true
	probe = &subsetValidationProbe{}
	if err := p.validateFrameSubset(context.Background(), probe, false, []*Frame{child}); err != nil || len(probe.rows) != 0 {
		t.Fatal("auxiliary validation filter lost", err)
	}
}
