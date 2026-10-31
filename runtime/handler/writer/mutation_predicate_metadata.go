package writer

import (
	"fmt"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

// Generated relation fields can carry controls without a separate child View.
// Parse the canonical view tag so invalid/duplicate groups fail just as they do
// for a standalone View; an explicit View must agree with its generated tag.
func recordMutationPredicate(viewTag string, view *spec.View) (*int, error) {
	parsed, err := dtag.ParseView(viewTag)
	if err != nil {
		return nil, err
	}
	var group *int
	if parsed != nil {
		group = parsed.MutationPredicateGroup
	}
	if view != nil && view.MutationPredicateGroup != nil {
		if *view.MutationPredicateGroup < 0 {
			return nil, fmt.Errorf("mutation predicate group must be nonnegative")
		}
		if group != nil && *group != *view.MutationPredicateGroup {
			return nil, fmt.Errorf("conflicting generated and explicit mutation predicate groups")
		}
		group = view.MutationPredicateGroup
	}
	if group == nil {
		return nil, nil
	}
	value := *group
	return &value, nil
}
