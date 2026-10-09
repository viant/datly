package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func phaseDescriptorFixture() *Reconciliation {
	return &Reconciliation{Mode: "source-phases", RootAction: "all-supplied-positive-keys", RootFields: []string{"Enabled"}, Roles: []ReconciliationRole{{Holder: "Children"}}, SourcePhases: &ReconciliationSourcePhases{
		Insert: []ReconciliationPhase{{Name: "children", Holder: "Children", Scope: "each-root", Workflow: "working-inserts", Followup: &ReconciliationRootFollowup{Placement: "after-phase", Fields: []string{"Enabled"}}}},
		Update: []ReconciliationPhase{{Name: "children", Holder: "Children", Scope: "each-root", Workflow: "working-updates-current-deletes-then-inserts", UpdateBasis: "current", Followup: &ReconciliationRootFollowup{Placement: "after-group", Fields: []string{"Enabled"}}}},
	}}
}
func TestSourcePhaseDescriptorCloneAndClosedAdmission(t *testing.T) {
	r := phaseDescriptorFixture()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseReconciliation(string(data))
	if err != nil {
		t.Fatal(err)
	}
	clone := parsed.Clone()
	parsed.SourcePhases.Insert[0].Followup.Fields[0] = "ID"
	parsed.SourcePhases.Update[0].Name = "changed"
	if clone.SourcePhases.Insert[0].Followup.Fields[0] != "Enabled" || clone.SourcePhases.Update[0].Name != "children" {
		t.Fatal("descriptor clone retains mutable authority")
	}
	if err = ValidateReconciliationView(&View{Reconciliation: clone}, "patch"); err == nil {
		t.Fatal("incomplete phase runtime admitted")
	}
}
func TestSourcePhaseDescriptorRejectsUnsupportedAuthority(t *testing.T) {
	cases := map[string]func(*Reconciliation){
		"old-mode":       func(r *Reconciliation) { r.Mode = "same-parent-root-first"; r.RootAction = "" },
		"missing-branch": func(r *Reconciliation) { r.SourcePhases.Update = nil },
		"unknown-holder": func(r *Reconciliation) { r.SourcePhases.Insert[0].Holder = "Other" },
		"duplicate-name": func(r *Reconciliation) {
			r.SourcePhases.Insert = append(r.SourcePhases.Insert, r.SourcePhases.Insert[0])
		},
		"implicit-root":             func(r *Reconciliation) { r.SourcePhases.Insert[0].Name = "root" },
		"unknown-scope":             func(r *Reconciliation) { r.SourcePhases.Insert[0].Scope = "arbitrary-order" },
		"unknown-workflow":          func(r *Reconciliation) { r.SourcePhases.Insert[0].Workflow = "execute-sql" },
		"missing-update-basis":      func(r *Reconciliation) { r.SourcePhases.Update[0].UpdateBasis = "" },
		"unknown-update-basis":      func(r *Reconciliation) { r.SourcePhases.Update[0].UpdateBasis = "forged-Current" },
		"basis-on-insert":           func(r *Reconciliation) { r.SourcePhases.Insert[0].UpdateBasis = "current" },
		"aggregate-followup":        func(r *Reconciliation) { r.SourcePhases.Insert[0].Scope = "all-roots" },
		"undeclared-followup-field": func(r *Reconciliation) { r.SourcePhases.Insert[0].Followup.Fields[0] = "ID" },
		"duplicate-followup-field": func(r *Reconciliation) {
			r.SourcePhases.Insert[0].Followup.Fields = append(r.SourcePhases.Insert[0].Followup.Fields, "Enabled")
		},
		"unknown-placement": func(r *Reconciliation) { r.SourcePhases.Insert[0].Followup.Placement = "anywhere" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := phaseDescriptorFixture()
			mutate(r)
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ParseReconciliation(string(b)); err == nil {
				t.Fatal("unsupported schedule accepted")
			}
		})
	}
}

func TestSourcePhaseDirectDescriptorCannotBypassOldModeGate(t *testing.T) {
	r := phaseDescriptorFixture()
	r.Mode = "same-parent-root-first"
	r.RootAction = ""
	err := ValidateReconciliationView(&View{Reconciliation: r}, "patch")
	if err == nil || !strings.Contains(err.Error(), "SourcePhases requires source-phases") {
		t.Fatal("authoring direct descriptor bypass", err)
	}
}
