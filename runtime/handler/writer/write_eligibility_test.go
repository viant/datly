package writer

import (
	"context"
	"errors"
	h "github.com/viant/xdatly/handler"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

var eligibilitySentinel = errors.New("eligibility sentinel")

type eligibleHas struct{ Name bool }
type eligibleRow struct {
	ID     int
	Name   string
	Locked bool
	Has    *eligibleHas
}
type eligibleInput struct{ Rows []*eligibleRow }
type eligibleOutput struct{ Data []*eligibleRow }
type eligibleProbe struct {
	mutate string
	calls  []h.WriteAction
	other  *eligibleRow
}

func (p *eligibleProbe) WriteEligible(_ context.Context, row *eligibleRow, state h.LifecycleContext[eligibleRow, h.NoParent, eligibleOutput], action h.WriteAction) (bool, error) {
	p.calls = append(p.calls, action)
	switch p.mutate {
	case "cross callback":
		if row.ID == 1 {
			p.other.Locked = true
		} else {
			row.Locked = false
		}
	case "mutation and error":
		row.Name = "changed"
		return false, eligibilitySentinel
	case "value":
		row.Name = "changed"
	case "loaded evidence":
		reflect.ValueOf(state.PreviousFields).Clear()
	case "marker":
		row.Has.Name = !row.Has.Name
	case "previous":
		state.Previous.Name = "changed"
	case "output":
		state.Output.Data = append(state.Output.Data, row)
	case "association":
		row.Has = &eligibleHas{Name: row.Has.Name}
	case "error":
		return false, errors.New("business eligibility failure")
	}
	return !row.Locked, nil
}
func eligibilityFixture(t *testing.T) (*Program, *eligibleProbe) {
	t.Helper()
	probe := &eligibleProbe{}
	root := &Record{EntityType: reflect.TypeFor[eligibleRow](), HookType: reflect.TypeFor[eligibleProbe](), Table: "eligible", Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	metadata := &Metadata{Root: root, Operation: "patch", InputField: 0}
	if err := validateWriteEligibilityHooks(metadata, reflect.TypeFor[eligibleOutput]()); err != nil {
		t.Fatal(err)
	}
	rows := []*eligibleRow{{ID: 1, Name: "one", Locked: true, Has: &eligibleHas{true}}, {ID: 2, Name: "two", Has: &eligibleHas{true}}}
	p := &Program{metadata: metadata, input: &eligibleInput{rows}, output: &eligibleOutput{}, frames: &MutationFrames{}, actions: &MutationActions{}}
	for i, row := range rows {
		action := h.WriteInsert
		if i == 1 {
			action = h.WriteUpdate
		}
		frame := &Frame{Record: root, Entity: reflect.ValueOf(row), Previous: reflect.ValueOf(&eligibleRow{Name: "previous", Has: &eligibleHas{}}), Hook: reflect.ValueOf(probe), Action: action}
		p.frames.Rows = append(p.frames.Rows, frame)
		item := &Action{Kind: action, Entity: frame.Entity}
		p.actions.Rows = append(p.actions.Rows, item)
		p.queueItems = append(p.queueItems, item)
	}
	return p, probe
}
func TestWriteEligibilityRetainsRowsAndFiltersOnlyQueue(t *testing.T) {
	p, probe := eligibilityFixture(t)
	if len(p.graphIndex().inserts) != 1 {
		t.Fatal("missing candidate producer")
	}
	if err := p.decideWriteEligibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(probe.calls) != 2 {
		t.Fatal("wrong callback count")
	}
	if len(p.graphIndex().inserts) != 0 {
		t.Fatal("excluded insert remains FK producer")
	}
	p.filterIneligibleActions()
	if len(p.actions.Rows) != 1 || len(p.queueItems) != 1 || p.actions.Rows[0].Entity.Interface().(*eligibleRow).ID != 2 {
		t.Fatal("wrong queue filtering")
	}
	if len(p.input.(*eligibleInput).Rows) != 2 || p.frames.Rows[0].Entity.Elem().FieldByName("ID").Int() != 1 {
		t.Fatal("body membership or identity changed")
	}
	// A new attempt re-evaluates decisions rather than inheriting exclusion.
	p.input.(*eligibleInput).Rows[0].Locked = false
	if err := p.decideWriteEligibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.frames.Rows[0].ExcludedWrite {
		t.Fatal("stale decision")
	}
}
func TestWriteEligibilityRejectsStateMutation(t *testing.T) {
	for _, mode := range []string{"value", "marker", "previous", "output", "association", "loaded evidence"} {
		t.Run(mode, func(t *testing.T) {
			p, probe := eligibilityFixture(t)
			probe.mutate = mode
			if err := p.decideWriteEligibility(context.Background()); err == nil || !strings.Contains(err.Error(), "changed writer") {
				t.Fatalf("mutation accepted: %v", err)
			}
		})
	}
}
func TestWriteEligibilityCancellationAndError(t *testing.T) {
	p, probe := eligibilityFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.decideWriteEligibility(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(probe.calls) != 0 {
		t.Fatal("called cancelled hook")
	}
	probe.mutate = "error"
	if err := p.decideWriteEligibility(context.Background()); err == nil || !strings.Contains(err.Error(), "business eligibility failure") {
		t.Fatal(err)
	}
}
func TestWriteEligibilityRequiresManagedInvocation(t *testing.T) {
	p, probe := eligibilityFixture(t)
	if err := p.evaluateWriteEligibility(context.Background()); err == nil || !strings.Contains(err.Error(), "invocation-owned") {
		t.Fatal(err)
	}
	if len(probe.calls) != 0 {
		t.Fatal("unguarded callback ran")
	}
}
func TestWriteEligibilityRuntimeAdmission(t *testing.T) {
	for _, mode := range []string{"auxiliary", "delete", "child", "nested"} {
		t.Run(mode, func(t *testing.T) {
			p, _ := eligibilityFixture(t)
			root := p.metadata.Root
			root.writeEligibility = false
			switch mode {
			case "auxiliary":
				root.Auxiliary = true
			case "delete":
				root.DeleteMarker = &Field{}
			case "child":
				root.Relations = []*Relation{{Child: &Record{}}}
			case "nested":
				root.HookType = nil
				root.Relations = []*Relation{{Child: &Record{Auxiliary: true, HookType: reflect.TypeFor[eligibleProbe]()}}}
			}
			if err := validateWriteEligibilityHooks(p.metadata, reflect.TypeFor[eligibleOutput]()); err == nil {
				t.Fatal("unsupported graph admitted")
			}
		})
	}
}
func TestEligibilitySnapshotCyclesMapsAndHiddenScalars(t *testing.T) {
	type node struct {
		Next   *node
		Values map[string][]int
		When   time.Time
		hidden int
	}
	n := &node{Values: map[string][]int{"b": {2}, "a": {}}, When: time.Unix(1, 0), hidden: 3}
	n.Next = n
	before, err := immutableValues([]reflect.Value{reflect.ValueOf(n)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		after, e := immutableValues([]reflect.Value{reflect.ValueOf(n)})
		if e != nil || after != before {
			t.Fatal("unstable cyclic/map snapshot", e)
		}
	}
	n.hidden = 4
	after, e := immutableValues([]reflect.Value{reflect.ValueOf(n)})
	if e != nil || after == before {
		t.Fatal("hidden mutation missed", e)
	}
	n.hidden = 3
	n.Values["a"] = nil
	after, e = immutableValues([]reflect.Value{reflect.ValueOf(n)})
	if e != nil || after == before {
		t.Fatal("nil/empty mutation missed", e)
	}
}

func TestEligibilitySnapshotOverlappingSlicesAndMapPointerCycle(t *testing.T) {
	values := []int{1, 2}
	graph := [][]int{values[:1], values[:2]}
	before, err := immutableValues([]reflect.Value{reflect.ValueOf(graph)})
	if err != nil {
		t.Fatal(err)
	}
	values[1] = 3
	after, err := immutableValues([]reflect.Value{reflect.ValueOf(graph)})
	if err != nil || before == after {
		t.Fatal("overlapping slice mutation missed", err)
	}
	type key struct{ Values map[*key]string }
	k := &key{}
	k.Values = map[*key]string{k: "cycle"}
	before, err = immutableValues([]reflect.Value{reflect.ValueOf(k)})
	if err != nil {
		t.Fatal(err)
	}
	after, err = immutableValues([]reflect.Value{reflect.ValueOf(k)})
	if err != nil || before != after {
		t.Fatal("map key cycle unstable", err)
	}
}

func TestEligibilityIdentityOnlyRowsAreValidatedTwice(t *testing.T) {
	p, _ := eligibilityFixture(t)
	p.frames.Rows = p.frames.Rows[1:]
	root := p.metadata.Root
	root.Keys = root.Fields
	frame := p.frames.Rows[0]
	frame.Fields = livePresence(root, frame.Entity)
	validator := &fakeCapabilities{}
	if err := p.validateFrames(context.Background(), validator, false); err != nil {
		t.Fatal(err)
	}
	if err := p.validateFrames(context.Background(), validator, true); err != nil {
		t.Fatal(err)
	}
	if len(validator.validations) != 2 || len(validator.validations[0]) != 1 || len(validator.validations[1]) != 1 {
		t.Fatal("identity-only eligibility validation omitted", validator.validations)
	}
	root.writeEligibility = false
	validator.validations = nil
	if err := p.validateFrames(context.Background(), validator, false); err != nil {
		t.Fatal(err)
	}
	if len(validator.validations) != 0 {
		t.Fatal("no-hook validation behavior changed")
	}
}
func TestEligibilityRejectsMutationBeforeNextCallback(t *testing.T) {
	p, probe := eligibilityFixture(t)
	probe.mutate = "cross callback"
	probe.other = p.input.(*eligibleInput).Rows[1]
	if err := p.decideWriteEligibility(context.Background()); err == nil {
		t.Fatal("cross-callback mutation accepted")
	}
	if len(probe.calls) != 1 {
		t.Fatal("second callback observed mutated body")
	}
}
func TestEligibilitySnapshotChecksObjectsReachableThroughMapKeys(t *testing.T) {
	type key struct{ Value int }
	k := &key{1}
	m := map[*key]string{k: "only reference"}
	before, err := immutableValues([]reflect.Value{reflect.ValueOf(m)})
	if err != nil {
		t.Fatal(err)
	}
	k.Value = 2
	after, err := immutableValues([]reflect.Value{reflect.ValueOf(m)})
	if err != nil || before == after {
		t.Fatal("map key object mutation missed", err)
	}
}

func TestEligibilityMutationPreservesCallbackCause(t *testing.T) {
	p, probe := eligibilityFixture(t)
	probe.mutate = "mutation and error"
	err := p.decideWriteEligibility(context.Background())
	if !errors.Is(err, eligibilitySentinel) || !strings.Contains(err.Error(), "changed writer") {
		t.Fatalf("lost callback or mutation cause: %v", err)
	}
}

func TestEligibilitySnapshotRejectsAmbiguousNaNMapKeys(t *testing.T) {
	nan := math.NaN()
	m := map[float64]string{}
	m[nan] = "one"
	m[nan] = "two"
	if _, err := immutableValues([]reflect.Value{reflect.ValueOf(m)}); err == nil {
		t.Fatal("ambiguous NaN keys admitted")
	}
}
