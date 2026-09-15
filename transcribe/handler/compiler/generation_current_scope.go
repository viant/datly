package compiler

import (
	"fmt"
	plan "github.com/viant/datly/transcribe/handler/ast"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// currentParentScope retains the existing canonical equality and bound parent
// read. It cannot expand a root's authored authorization scope.
type currentParentScope struct {
	view     *spec.View
	relation *spec.Relation
	input    string
}

// addScopedCurrent loads children reachable from the authorized parent read,
// including existing children whose IDs were omitted from the request. Every
// deeper read repeats this rule against its immediate parent's scoped result.
func (b *inputGeneration) addScopedCurrent(view *spec.View, name string, scope *currentParentScope) error {
	links, err := compileKeyLinks(scope.view, scope.relation)
	if err != nil {
		return err
	}
	if len(links) == 0 || scope.input == "" {
		return fmt.Errorf("child Previous %s requires a bounded parent equality", name)
	}
	var columns []string
	for _, link := range links {
		column, err := resolveLinkColumn(view, link.Child.Source)
		if err != nil {
			return err
		}
		columns = append(columns, column.Name)
	}
	method := "Project" + typecatalog.FieldName(name) + "ParentKeys"
	if err = b.appendCurrent(view, name, "$criteria.CompositeIn(\"r\", $Unsafe."+method+"($"+scope.input+"))"); err != nil {
		return err
	}
	identity, err := view.Identity()
	if err != nil {
		return err
	}
	// Existing CurrentBinding metadata carries the typed projection; no second
	// binding graph or hand-authored request/key parameters are introduced.
	b.request.Currents = append(b.request.Currents, CurrentBinding{ViewIdentity: identity, Param: name, Lookup: &plan.LookupProjection{Name: method, Columns: columns, ParentOnly: true}})
	return nil
}
