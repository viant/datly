package exec

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// RelationSelection is an immutable, trusted invocation control. It narrows
// relation loading independently of scalar SQL projection and grants no access.
type RelationSelection struct {
	target  ComponentTarget
	view    string
	holders []string
}
type relationSelectionKey struct{}

// RelationSelectionValidator validates the current capability against compiled
// reader metadata before the runtime starts input dependency binding.
type RelationSelectionValidator interface {
	ValidateRelationSelection(context.Context) error
}

// NewRelationSelection selects authored holders on one exact component/view.
// An empty list explicitly selects no relations; a nil capability preserves
// ordinary selector behavior. Names are resolved by the native reader metadata.
func NewRelationSelection(target ComponentTarget, view string, holders ...string) *RelationSelection {
	target.Route.Method = strings.ToUpper(strings.TrimSpace(target.Route.Method))
	target.Route.Path = strings.TrimSpace(target.Route.Path)
	return &RelationSelection{target: target, view: strings.TrimSpace(view), holders: append([]string{}, holders...)}
}

// EnterRelationSelection shadows parent state at every component boundary.
// A dependency or nested invocation must explicitly supply its own capability.
func EnterRelationSelection(ctx context.Context, target ComponentTarget, selection *RelationSelection) (context.Context, error) {
	if selection != nil && (selection.target != target || selection.view == "") {
		return ctx, fmt.Errorf("relation selection does not match target %s", target.String())
	}
	var value RelationSelection
	if selection != nil {
		value = *selection
	}
	return context.WithValue(ctx, relationSelectionKey{}, value), nil
}

// SelectedRelations returns detached selection metadata for its exact owner.
// The bool distinguishes an explicit empty set from ordinary relation loading.
func SelectedRelations(ctx context.Context, component spec.Key) (string, []string, bool) {
	selection, _ := ctx.Value(relationSelectionKey{}).(RelationSelection)
	if selection.view == "" || selection.target.Component != component {
		return "", nil, false
	}
	return selection.view, append([]string{}, selection.holders...), true
}
