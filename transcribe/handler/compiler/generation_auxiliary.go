package compiler

import (
	"fmt"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// auxiliary derives a lookup from its authored parent equality. Current-parent
// values come from the already bound, scoped read; no second table read supplies
// missing FKs. A typed Go projection feeds the normal SQL fragment owner.
func (b *inputGeneration) auxiliary(parent *spec.View, relation *spec.Relation, body string) error {
	view := relation.View
	identity, err := view.Identity()
	if err != nil {
		return err
	}
	if b.visiting[identity] {
		return fmt.Errorf("auxiliary lookup %s has multiple parent roles", identity)
	}
	b.visiting[identity] = true
	currentName, err := b.currentName(view)
	if err != nil {
		return err
	}
	if existing := b.names[currentName]; existing != nil {
		if existing.Source.Kind != "view" || b.request.ViewBindings[existing.Identity()] == "" {
			return fmt.Errorf("auxiliary current %s conflicts with authored binding", currentName)
		}
		b.request.Currents = append(b.request.Currents, CurrentBinding{ViewIdentity: identity, Param: currentName})
		return nil
	}
	if len(view.Relations) > 0 {
		return fmt.Errorf("auxiliary lookup %s with nested relations requires explicit authored current authority", identity)
	}
	links, err := compileKeyLinks(parent, relation)
	if err != nil {
		return err
	}
	parentIdentity, err := parent.Identity()
	if err != nil {
		return err
	}
	var currentParent string
	for _, binding := range b.request.Currents {
		if binding.ViewIdentity == parentIdentity {
			currentParent = binding.Param
		}
	}
	if currentParent == "" {
		return fmt.Errorf("auxiliary lookup %s requires current-parent authority", identity)
	}
	if len(parent.Columns) == 0 {
		return fmt.Errorf("auxiliary lookup %s has no parent columns", identity)
	}
	method := "Project" + typecatalog.FieldName(currentName) + "Keys"
	columns := make([]string, 0, len(links))
	for _, link := range links {
		col, err := resolveLinkColumn(view, link.Child.Source)
		if err != nil {
			return err
		}
		columns = append(columns, col.Name)
	}
	predicate := "$criteria.CompositeIn(\"r\", $Unsafe." + method + "($" + body + ", $" + currentParent + "))"

	if err := b.appendCurrent(view, currentName, predicate); err != nil {
		return err
	}
	b.request.Currents = append(b.request.Currents, CurrentBinding{ViewIdentity: identity, Param: currentName, Lookup: &plan.LookupProjection{Name: method, Columns: columns}})
	return nil
}
