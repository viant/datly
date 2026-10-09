package writer

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

func finiteDecisionFixture(t *testing.T, rows []*qcRow, current []*qcRow) (*Handler, *Program, *qcInput) {
	t.Helper()
	h, in, _, _, _, _ := qcFixture(t, false, "", "patch")
	h.metadata.Root.QueueContract = ""
	h.metadata.Root.HookType = nil
	h.metadata.Root.DeleteMarker = nil
	input := in.Input.(*qcInput)
	input.Rows, input.CurrentRows = rows, current
	compiled, err := compileFiniteRootDecision(h.metadata.Root)
	if err != nil {
		t.Fatal(err)
	}
	h.metadata.Root.reconciliation = &reconciliationMetadata{rootDecision: compiled}
	p, err := h.program(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.indexRecordCurrent(reflect.ValueOf(input).Elem(), h.metadata.Root); err != nil {
		t.Fatal(err)
	}
	return h, p, input
}

func TestFiniteRootDecisionOriginalRequestFacts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ids     []int
		marked  []bool
		current []int
		action  xhandler.WriteAction
		missing bool
	}{
		{name: "empty", action: xhandler.WriteInsert},
		{name: "fresh", ids: []int{0, 0}, marked: []bool{false, false}, action: xhandler.WriteInsert},
		{name: "all existing", ids: []int{1, 2}, marked: []bool{true, true}, current: []int{1, 2}, action: xhandler.WriteUpdate},
		{name: "mixed", ids: []int{1, 0}, marked: []bool{true, false}, current: []int{1}, action: xhandler.WriteInsert},
		{name: "unmarked positive", ids: []int{1}, marked: []bool{false}, current: []int{1}, action: xhandler.WriteInsert},
		{name: "marked zero", ids: []int{0}, marked: []bool{true}, action: xhandler.WriteInsert},
		{name: "negative", ids: []int{1, -1}, marked: []bool{true, true}, current: []int{1}, action: xhandler.WriteInsert},
		{name: "missing Current", ids: []int{1, 9}, marked: []bool{true, true}, current: []int{1}, action: xhandler.WriteUpdate, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rows, current []*qcRow
			for n, id := range tc.ids {
				rows = append(rows, &qcRow{ID: id, Has: &qcHas{ID: tc.marked[n], Name: true}})
			}
			for _, id := range tc.current {
				current = append(current, &qcRow{ID: id, Name: "loaded"})
			}
			h, p, input := finiteDecisionFixture(t, rows, current)
			if p.finiteRootDecision.action != tc.action {
				t.Fatal(p.finiteRootDecision.action)
			}
			for pass := 0; pass < 2; pass++ {
				p.frames = &MutationFrames{}
				err := p.buildRecordFrames(context.Background(), nil, h.metadata.Root, reflect.ValueOf(input.Rows), nil)
				if tc.missing {
					if err == nil || !strings.Contains(err.Error(), "requires real Current") {
						t.Fatal(err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(p.frames.Rows) != len(rows) {
					t.Fatal("root population changed")
				}
				for _, frame := range p.frames.Rows {
					if frame.Action != tc.action {
						t.Fatal(frame.Action)
					}
					if tc.name == "mixed" && frame.Entity.Pointer() == reflect.ValueOf(rows[0]).Pointer() && !frame.Previous.IsValid() {
						t.Fatal("real Previous lost")
					}
				}
			}
		})
	}
}

func TestFiniteRootDecisionPreallocationSeals(t *testing.T) {
	for _, change := range []string{"append", "remove", "reorder", "replace", "key", "marker", "missing marker"} {
		t.Run(change, func(t *testing.T) {
			rows := []*qcRow{{ID: 1, Has: &qcHas{ID: true}}, {ID: 2, Has: &qcHas{ID: true}}}
			_, p, input := finiteDecisionFixture(t, rows, nil)
			switch change {
			case "append":
				input.Rows = append(input.Rows, &qcRow{})
			case "remove":
				input.Rows = input.Rows[:1]
			case "reorder":
				input.Rows[0], input.Rows[1] = input.Rows[1], input.Rows[0]
			case "replace":
				input.Rows[0] = &qcRow{ID: 1, Has: &qcHas{ID: true}}
			case "key":
				input.Rows[0].ID = 3
			case "marker":
				input.Rows[0].Has.ID = false
			case "missing marker":
				input.Rows[0].Has = nil
			}
			if err := p.validateFiniteRootDecision(reflect.ValueOf(input.Rows)); err == nil {
				t.Fatal("changed source facts admitted")
			}
		})
	}
	row := &qcRow{ID: 1, Has: &qcHas{ID: true}}
	_, p, input := finiteDecisionFixture(t, []*qcRow{row, row}, nil)
	if len(p.finiteRootDecision.occurrences) != 2 {
		t.Fatal("duplicate pointer collapsed occurrences")
	}
	row.Name = "business default"
	row.Has.Name = true
	if err := p.validateFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
		t.Fatal(err)
	}
	if p.finiteRootDecision.action != xhandler.WriteUpdate {
		t.Fatal("business default changed action")
	}
}

func TestFiniteRootInsertPreviousIsEvidenceOnly(t *testing.T) {
	_, p, input := finiteDecisionFixture(t, []*qcRow{{ID: 1, Has: &qcHas{Name: true}}}, []*qcRow{{ID: 1, Name: "loaded", Note: new(int)}})
	if err := p.buildRecordFrames(context.Background(), nil, p.metadata.Root, reflect.ValueOf(input.Rows), nil); err != nil {
		t.Fatal(err)
	}
	frame := p.frames.Rows[0]
	if !frame.Previous.IsValid() || frame.Action != xhandler.WriteInsert {
		t.Fatal("real evidence/insert decision lost")
	}
	p.metadata.Root.Invariants = map[string][]Field{"pair": p.metadata.Root.Fields}
	if err := p.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if input.Rows[0].Note != nil {
		t.Fatal("insert backfilled from matched Current")
	}
	options := p.validationOptions(frame, false)
	if options.Previous != nil || options.Action != xhandler.WriteInsert {
		t.Fatal("insert used update validation authority")
	}
}

func TestFiniteRootDecisionDoesNotOverrideChildAction(t *testing.T) {
	h, p, _ := finiteDecisionFixture(t, []*qcRow{{ID: 0, Has: &qcHas{}}}, nil)
	child := *h.metadata.Root
	child.reconciliation = nil
	child.Relations = nil
	parent := &Frame{Record: h.metadata.Root, Entity: reflect.ValueOf(&qcRow{ID: 0})}
	h.metadata.Root.Relations = []*Relation{{Child: &child}}
	row := &qcRow{ID: 2, Has: &qcHas{ID: true}}
	key, _ := child.key(reflect.ValueOf(row).Elem())
	p.database.Rows[rowIdentity{record: &child, key: key}] = reflect.ValueOf(&qcRow{ID: 2})
	if err := p.buildEntityFrame(context.Background(), nil, &child, reflect.ValueOf(row), parent, 0, true); err != nil {
		t.Fatal(err)
	}
	if p.frames.Rows[0].Action != xhandler.WriteUpdate {
		t.Fatal("root INSERT propagated to child")
	}
}

func TestFiniteRootDecisionUnsupportedMetadata(t *testing.T) {
	h, _, _, _, _, _ := qcFixture(t, false, "", "patch")
	root := h.metadata.Root
	key := root.Keys[0]
	for _, mode := range []string{"composite", "no sequence", "no marker", "not autoincrement"} {
		copy := *root
		copy.Keys = []Field{key}
		switch mode {
		case "composite":
			copy.Keys = append(copy.Keys, key)
		case "no sequence":
			copy.Sequence = nil
		case "no marker":
			copy.Keys[0].Has = nil
		case "not autoincrement":
			sequence := *copy.Sequence
			sequence.AutoIncrement = false
			copy.Sequence = &sequence
		}
		if _, err := compileFiniteRootDecision(&copy); err == nil {
			t.Fatal(mode)
		}
	}
	compiled, err := compileFiniteRootDecision(root)
	if err != nil {
		t.Fatal(err)
	}
	root.reconciliation = &reconciliationMetadata{rootDecision: compiled}
	p := &Program{metadata: h.metadata}
	if err = p.captureFiniteRootDecision(reflect.ValueOf([]*qcRow{nil, {ID: 1, Has: &qcHas{ID: true}}})); err == nil {
		t.Fatal("unsupported null admitted")
	}
	if p.finiteRootDecision.action != xhandler.WriteInsert {
		t.Fatal("nil occurrence ignored in original decision")
	}
}

type finitePreflightHook struct{ visits int }

func (h *finitePreflightHook) Init(_ context.Context, row *qcRow, _ xhandler.LifecycleContext[qcRow, xhandler.NoParent, qcOutput]) error {
	h.visits++
	row.Name = "must not happen before all roots have Current"
	return nil
}

func TestFiniteRootMissingCurrentPrecedesAllEntityInit(t *testing.T) {
	h, p, input := finiteDecisionFixture(t, []*qcRow{{ID: 1, Name: "first", Has: &qcHas{ID: true}}, {ID: 9, Name: "missing", Has: &qcHas{ID: true}}}, []*qcRow{{ID: 1, Name: "loaded"}})
	hook := &finitePreflightHook{}
	h.metadata.Root.HookType = reflect.TypeFor[finitePreflightHook]()
	p.hook = reflect.ValueOf(hook)
	p.hooksByRecord[h.metadata.Root] = p.hook
	// A nil binder would reject hooks later; missing Current must precede even
	// that boundary, and no earlier root is allowed to run its Init.
	// The fixture indexes Current for helper-level tests; prepare must own
	// its single production indexing pass.
	p.database = &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}, ByRecord: map[*Record][]reflect.Value{}}
	err := p.prepare(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "requires real Current at occurrence 1") {
		t.Fatal(err)
	}
	if hook.visits != 0 || input.Rows[0].Name != "first" || len(p.actions.Rows) != 0 {
		t.Fatal("earlier root work happened before request-wide Current preflight")
	}
}
