package writer

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type phaseTestHas struct{ ID, Enabled bool }
type phaseTestRoot struct {
	ID       int
	Enabled  bool
	Children []*phaseTestChild
	Details  []*phaseTestChild
	Has      *phaseTestHas
}
type phaseTestChild struct {
	ID, ParentID int
	Value        string
	Pointer      *string
	Has          *phaseTestHas `setMarker:"true"`
}

func finitePhaseFixture() (*Record, *spec.Reconciliation) {
	key := Field{Name: "ID", Index: []int{0}, Has: []int{4, 0}, AutoIncrement: true}
	root := &Record{Path: "Rows", Table: "items", EntityType: reflect.TypeFor[phaseTestRoot](), Keys: []Field{key}, Sequence: &key, Fields: []Field{key, {Name: "Enabled", Index: []int{1}}}, CurrentField: 0}
	child := &Record{Path: "Rows/Children", Table: "children", EntityType: reflect.TypeFor[phaseTestChild](), Keys: []Field{key}, Fields: []Field{key, {Name: "ParentID", Index: []int{1}}, {Name: "Value", Index: []int{2}}, {Name: "Pointer", Index: []int{3}}}, CurrentField: 1}
	detail := *child
	detail.Path = "Rows/Details"
	detail.Table = "details"
	root.Relations = []*Relation{{Field: []int{2}, Child: child, Links: []Link{{Parent: key, Child: Field{Name: "ParentID", Index: []int{1}}}}}, {Field: []int{3}, Child: &detail, Links: []Link{{Parent: key, Child: Field{Name: "ParentID", Index: []int{1}}}}}}
	phases := &spec.ReconciliationSourcePhases{
		Insert: []spec.ReconciliationPhase{{Name: "details", Holder: "Details", Scope: "all-roots", Workflow: "working-inserts"}, {Name: "other-details", Holder: "Details", Scope: "all-roots", Workflow: "working-updates-then-inserts", UpdateBasis: "working"}, {Name: "children", Holder: "Children", Scope: "each-root", Workflow: "working-inserts", Followup: &spec.ReconciliationRootFollowup{Placement: "after-phase", Fields: []string{"Enabled"}}}},
		Update: []spec.ReconciliationPhase{{Name: "details", Holder: "Details", Scope: "all-roots", Workflow: "working-updates-then-inserts", UpdateBasis: "working"}, {Name: "children", Holder: "Children", Scope: "each-root", Workflow: "working-updates-current-deletes-then-inserts", UpdateBasis: "current", Followup: &spec.ReconciliationRootFollowup{Placement: "after-group", Fields: []string{"Enabled"}}}},
	}
	return root, &spec.Reconciliation{Mode: "source-phases", RootAction: "all-supplied-positive-keys", RootFields: []string{"Enabled"}, Roles: []spec.ReconciliationRole{{Holder: "Children", Fields: []string{"Value", "Pointer"}}, {Holder: "Details", Fields: []string{"Value", "Pointer"}}}, SourcePhases: phases}
}
func TestFiniteSourcePhaseCompilerPreservesAuthorityAndBoundaries(t *testing.T) {
	root, decl := finitePhaseFixture()
	compiled, err := compileFiniteSourcePhases(root, decl)
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.insert) != 3 || len(compiled.update) != 2 || compiled.insert[0].role != root.Relations[1] || compiled.insert[1].role != root.Relations[1] {
		t.Fatal("phase order or reused physical ownership lost")
	}
	if compiled.insert[2].scope != "each-root" || compiled.insert[2].followup.placement != "after-phase" || compiled.update[1].followup.placement != "after-group" || compiled.update[1].updateBasis != "current" {
		t.Fatal("branch boundaries lost")
	}
	decl.SourcePhases.Insert[2].Followup.Fields[0] = "ID"
	decl.SourcePhases.Update[1].UpdateBasis = "working"
	decl.Roles[0].Fields[0] = "ID"
	if compiled.insert[2].followup.fields["Enabled"].Name != "Enabled" || compiled.update[1].updateBasis != "current" || compiled.update[1].fields["Value"].Name != "Value" {
		t.Fatal("mutable descriptor altered compiled authority")
	}
	if root.reconciliation != nil {
		t.Fatal("internal compile activated unsupported runtime")
	}
}
func TestFiniteSourcePhaseCompilerRejectsNoncanonicalGraphs(t *testing.T) {
	cases := map[string]func(*Record, *spec.Reconciliation){
		"no-root-table":        func(r *Record, _ *spec.Reconciliation) { r.Table = "" },
		"no-root-type":         func(r *Record, _ *spec.Reconciliation) { r.EntityType = nil },
		"no-real-root-Current": func(r *Record, _ *spec.Reconciliation) { r.CurrentField = -1 },
		"non-auto-root":        func(r *Record, _ *spec.Reconciliation) { r.Sequence = nil },
		"missing-Current":      func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Child.CurrentField = -1 },
		"auxiliary-role":       func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Child.Auxiliary = true },
		"nested-role":          func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Child.Relations = []*Relation{{}} },
		"missing-link":         func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Links = nil },
		"wrong-holder-type":    func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Field = []int{1} },
		"bad-field-index":      func(r *Record, _ *spec.Reconciliation) { r.Relations[0].Field = []int{99} },
		"unknown-role":         func(_ *Record, d *spec.Reconciliation) { d.Roles[0].Holder = "Other" },
		"identity-assignment": func(_ *Record, d *spec.Reconciliation) {
			d.RootFields = []string{"ID"}
			d.SourcePhases.Insert[2].Followup.Fields = []string{"ID"}
			d.SourcePhases.Update[1].Followup.Fields = []string{"ID"}
		},
		"unproved-adoption": func(_ *Record, d *spec.Reconciliation) { d.Roles[0].AdoptIdentity = true },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r, d := finitePhaseFixture()
			mutate(r, d)
			if _, err := compileFiniteSourcePhases(r, d); err == nil {
				t.Fatal("unsupported authority compiled")
			}
		})
	}
}

func TestFiniteSourcePhaseMetadataCannotBypassOldModeGate(t *testing.T) {
	r, d := finitePhaseFixture()
	d.Mode = "same-parent-root-first"
	d.RootAction = ""
	metadata := &Metadata{Root: r, Component: &spec.Component{RootView: &spec.View{Reconciliation: d}}}
	err := validateReconciliation(metadata, reflect.TypeFor[qcInput](), reflect.TypeFor[qcOutput]())
	if err == nil || !strings.Contains(err.Error(), "SourcePhases requires source-phases") {
		t.Fatal("native direct descriptor bypass", err)
	}
}
