package dml

import "github.com/viant/datly/internal/drainowner"

// ValidateBoundExecutionGuard is an optional Datly-internal identity check.
// A component view must be the original enrolled pointer, not a copied view
// that merely names the same owner. It grants no continuing write authority.
func (d *Data) ValidateBoundExecutionGuard(binding *drainowner.GuardBinding) error {
	if d == nil {
		return drainowner.ErrOwner
	}
	owner := d.owner()
	if err := drainowner.ValidateGuardOwner(owner, binding); err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.invocation || !owner.open || owner.completed || owner.mutationAdmissionClosed || owner.failed != nil {
		return drainowner.ErrGuardBinding
	}
	if err := drainowner.ProtectedOwnerFailure(owner); err != nil {
		return err
	}
	// Walk upward with canonical membership checks; each parent must retain the
	// exact child in its native journal tree. This also excludes closed ancestors.
	seen := map[*Data]bool{}
	for view := d; view != owner; view = view.parent {
		if view == nil || seen[view] || !view.open || view.root != owner || view.parent == nil || !view.parent.open || view.parent.owner() != owner {
			return drainowner.ErrOwner
		}
		seen[view] = true
		found := 0
		for _, child := range view.parent.bindings {
			if child == view {
				found++
			}
		}
		for _, marker := range view.parent.markers {
			if marker.child == view {
				found++
			}
		}
		if found != 1 {
			return drainowner.ErrOwner
		}
	}
	return nil
}
