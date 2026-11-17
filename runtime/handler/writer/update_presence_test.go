package writer

import "testing"

// Validation coverage can outlive a supplied marker when normalization resolves
// the triggering field to Previous. Hydrated companions must never become writes.
func TestUpdateEligibilityUsesWritePresenceNotInvariantCoverage(t *testing.T) {
	p, frame, row := auditFixture()
	if err := p.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if row.Has.Qty || !frame.Fields.Has("Qty") {
		t.Fatal("invariant companion must be validation-only")
	}
	if !frame.Fields.excluding([]string{"ID"}).Has("Qty") {
		t.Fatal("reference exclusions dropped unrelated invariant coverage")
	}
	if !hasMutableFields(frame) {
		t.Fatal("marked business change was lost")
	}
	// The existing presence synchronizer / business normalizer clears this bit.
	row.Name = frame.Previous.Interface().(*auditRow).Name
	row.Has.Name = false
	if hasMutableFields(frame) {
		t.Fatal("validation-only invariant coverage made an unchanged row mutable")
	}
	if err := p.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if frame.Fields.Has("Qty") || frame.Fields.Has("Name") {
		t.Fatal("inactive invariant retained validation coverage after normalization")
	}
	if !row.Has.ID || !row.Has.Version {
		t.Fatal("identity and expected concurrency markers must be preserved")
	}
	row.Qty++
	row.Has.Qty = true
	if !hasMutableFields(frame) {
		t.Fatal("real companion change must remain writable")
	}
	if err := p.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if row.Has.Name || !frame.Fields.Has("Name") {
		t.Fatal("changed companion must reactivate validation without writing Name")
	}
}
