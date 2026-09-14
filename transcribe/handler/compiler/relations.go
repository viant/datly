package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

type recordContext struct {
	request        Request
	operation      plan.Operation
	view           *spec.View
	identity       string
	inputPath      plan.FieldPath
	destination    plan.FieldPath
	sequencePrefix plan.FieldPath
	cardinality    spec.Cardinality
	stack          map[string]bool
	auxiliary      bool
}

func (c *compiler) compileRelations(parent recordContext) ([]*plan.RelationPlan, error) {
	result := make([]*plan.RelationPlan, 0, len(parent.view.Relations))
	for _, relation := range parent.view.Relations {
		if relation == nil {
			return nil, fmt.Errorf("view %q contains a nil relation", parent.view.CanonicalName())
		}
		if relation.Kind == spec.RelationKindDerived {
			continue
		}
		if relation.View == nil {
			return nil, fmt.Errorf("view %q relation %q has no child view", parent.view.CanonicalName(), relation.Name)
		}
		holder := typecatalog.FieldName(relation.Holder)
		if holder == "" {
			holder = typecatalog.FieldName(relation.Name)
		}
		if holder == "" {
			return nil, fmt.Errorf("view %q relation %q requires a canonical holder field", parent.view.CanonicalName(), relation.Name)
		}
		cardinality := relation.Cardinality
		if cardinality != spec.CardinalityOne && cardinality != spec.CardinalityMany {
			return nil, fmt.Errorf("view %q relation %q requires explicit one or many cardinality", parent.view.CanonicalName(), relation.Name)
		}
		identity, err := relation.View.Identity()
		if err != nil {
			return nil, fmt.Errorf("view %q relation %q identity: %w", parent.view.CanonicalName(), relation.Name, err)
		}
		if parent.stack[identity] {
			return nil, fmt.Errorf("write relation cycle reaches view %q", identity)
		}
		links, err := compileKeyLinks(parent.view, relation)
		if err != nil {
			return nil, err
		}
		childContext := recordContext{
			request: parent.request, operation: parent.operation, view: relation.View, identity: identity,
			inputPath: appendPath(parent.inputPath, holder), destination: parent.destination,
			sequencePrefix: appendPath(parent.sequencePrefix, holder), cardinality: cardinality, stack: parent.stack,
			auxiliary: parent.auxiliary || relation.View.Auxiliary,
		}
		child, err := c.compileChildRecord(childContext)
		if err != nil {
			return nil, fmt.Errorf("compile relation %q: %w", relation.Name, err)
		}
		parent.stack[identity] = true
		child.Relations, err = c.compileRelations(childContext)
		delete(parent.stack, identity)
		if err != nil {
			return nil, err
		}
		relationIdentity := strings.TrimSpace(relation.Name)
		if relationIdentity == "" {
			relationIdentity = holder
		}
		result = append(result, &plan.RelationPlan{
			Identity: relationIdentity, FieldPath: plan.FieldPath{holder}, Cardinality: cardinality,
			Links: links, Child: child,
		})
	}
	return result, nil
}

func (c *compiler) compileChildRecord(current recordContext) (*plan.RecordPlan, error) {
	table := ""
	if current.view.Source != nil {
		table = strings.TrimSpace(current.view.Source.Table)
	}
	if table == "" && !current.auxiliary {
		return nil, fmt.Errorf("writable view %q requires a canonical table", current.view.CanonicalName())
	}
	keys, err := canonicalKeys(current.view)
	if err != nil {
		return nil, err
	}
	record := &plan.RecordPlan{
		Auxiliary: current.auxiliary,
		Identity:  current.identity, InputPath: clonePath(current.inputPath), Table: table,
		Cardinality: current.cardinality, Keys: keys,
		Write: plan.WritePolicy{ValuePath: clonePath(current.inputPath), Order: c.nextWriteOrder()},
	}
	switch current.operation {
	case plan.OperationPost:
		record.Write.Missing = plan.ActionInsert
		record.Write.Allowed = []plan.Action{plan.ActionInsert}
	case plan.OperationPut:
		if len(keys) == 0 && !record.Auxiliary {
			return nil, fmt.Errorf("PUT handler transcription requires a key for view %q", current.view.CanonicalName())
		}
		record.Write.Existing = plan.ActionUpdate
		record.Write.Allowed = []plan.Action{plan.ActionUpdate}
	case plan.OperationPatch:
		record.Write.Existing = plan.ActionUpdate
		record.Write.Missing = plan.ActionInsert
		record.Write.Allowed = []plan.Action{plan.ActionInsert, plan.ActionUpdate}
	}
	if current.operation == plan.OperationPatch || current.operation == plan.OperationPut {
		currentBinding, ok := c.currentByView[current.identity]
		if !ok && current.operation == plan.OperationPatch && !record.Auxiliary {
			return nil, fmt.Errorf("PATCH writable view %q requires an explicit current binding", current.identity)
		}
		if ok {
			c.usedCurrent[current.identity] = true
			record.Current, err = c.compileCurrent(current.request, currentBinding.name, current.view, record)
			if err != nil {
				return nil, err
			}
		}
	}
	if record.Auxiliary {
		record.Write = plan.WritePolicy{ValuePath: clonePath(current.inputPath), Order: record.Write.Order}
	} else {
		record.Sequence, err = sequencePlan(current.view, "", current.operation, current.destination, current.sequencePrefix)
		if err != nil {
			return nil, err
		}
		record.SelfRelations, err = compileSelfRelations(current.view)
		if err != nil {
			return nil, err
		}
	}
	return record, nil
}

func compileKeyLinks(parent *spec.View, relation *spec.Relation) ([]plan.KeyLink, error) {
	if len(relation.On) == 0 {
		return nil, fmt.Errorf("view %q relation %q requires at least one canonical key link", parent.CanonicalName(), relation.Name)
	}
	result := make([]plan.KeyLink, 0, len(relation.On))
	for _, link := range relation.On {
		if link == nil {
			return nil, fmt.Errorf("view %q relation %q contains a nil key link", parent.CanonicalName(), relation.Name)
		}
		parentColumn, err := resolveLinkColumn(parent, link.ParentColumn)
		if err != nil {
			return nil, fmt.Errorf("relation %q parent link: %w", relation.Name, err)
		}
		childColumn, err := resolveLinkColumn(relation.View, link.ChildColumn)
		if err != nil {
			return nil, fmt.Errorf("relation %q child link: %w", relation.Name, err)
		}
		parentType := parentColumn.EffectiveType()
		childType := childColumn.EffectiveType()
		if !sameBaseType(parentType, childType) {
			return nil, fmt.Errorf("relation %q link %q=%q has incompatible types %s.%s and %s.%s",
				relation.Name, parentColumn.Name, childColumn.Name,
				parentColumn.Type.Package, parentColumn.Type.Name, childColumn.Type.Package, childColumn.Type.Name)
		}
		result = append(result, plan.KeyLink{
			Parent: keyPart(parentColumn), Child: keyPart(childColumn),
			ParentNamespace: link.ParentNamespace, ChildNamespace: link.ChildNamespace,
			Conversion: linkConversion(parentType, childType),
		})
	}
	return result, nil
}

func resolveLinkColumn(view *spec.View, reference string) (*spec.Column, error) {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return nil, fmt.Errorf("view %q link column is required", view.CanonicalName())
	}
	var matched *spec.Column
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(column.Name), reference) && !strings.EqualFold(strings.TrimSpace(column.Source), reference) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("view %q link column %q is ambiguous", view.CanonicalName(), reference)
		}
		matched = column
	}
	if matched == nil {
		return nil, fmt.Errorf("view %q has no link column %q", view.CanonicalName(), reference)
	}
	if typecatalog.FieldName(matched.Name) == "" {
		return nil, fmt.Errorf("view %q link column %q has no canonical Go field", view.CanonicalName(), reference)
	}
	return matched, nil
}

func keyPart(column *spec.Column) plan.KeyPart {
	return plan.KeyPart{Field: typecatalog.FieldName(column.Name), Source: strings.TrimSpace(column.Source), Type: column.EffectiveType()}
}

func sameBaseType(left, right spec.TypeRef) bool {
	return strings.TrimSpace(left.Package) == strings.TrimSpace(right.Package) &&
		strings.TrimPrefix(strings.TrimSpace(left.Name), "*") == strings.TrimPrefix(strings.TrimSpace(right.Name), "*")
}

func linkConversion(parent, child spec.TypeRef) plan.LinkConversion {
	parentPointer := parent.Pointer || strings.HasPrefix(strings.TrimSpace(parent.Name), "*")
	childPointer := child.Pointer || strings.HasPrefix(strings.TrimSpace(child.Name), "*")
	switch {
	case !parentPointer && childPointer:
		return plan.LinkAddress
	case parentPointer && !childPointer:
		return plan.LinkDereference
	default:
		return plan.LinkDirect
	}
}

func appendPath(path plan.FieldPath, parts ...string) plan.FieldPath {
	result := clonePath(path)
	return append(result, parts...)
}
