package compiler

import (
	"fmt"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
	"strings"
)

func compileSelfRelations(view *spec.View) ([]plan.SelfRelationPlan, error) {
	if view.SelfReference == nil {
		return nil, nil
	}
	self := view.SelfReference
	if self.Holder == "" {
		return nil, fmt.Errorf("self relation in view %s requires a holder", view.CanonicalName())
	}
	links, err := compileKeyLinks(view, &spec.Relation{Name: self.Holder, View: view, On: []*spec.RelationLink{{ParentColumn: self.Child, ChildColumn: self.Parent}}})
	if err != nil {
		return nil, err
	}
	var holder plan.FieldPath
	for _, part := range strings.Split(self.Holder, ".") {
		name := typecatalog.FieldName(part)
		if name == "" {
			return nil, fmt.Errorf("self relation in view %s has an invalid holder path", view.CanonicalName())
		}
		holder = append(holder, name)
	}
	return []plan.SelfRelationPlan{{FieldPath: holder, Links: links}}, nil
}
