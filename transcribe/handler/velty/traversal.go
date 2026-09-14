package velty

import (
	"fmt"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

type traversalLowerer struct {
	operation plan.Operation
	nextIndex int
}
type recordScope struct {
	record        *plan.RecordPlan
	source, local selector
	indexes       []expression
	relation      *plan.RelationPlan
	parent        *recordScope
}

func (l *traversalLowerer) traverse(program *block, scope recordScope) error {
	if scope.record.Auxiliary {
		return nil
	}
	if scope.record.Cardinality == spec.CardinalityMany {
		values := scope.source
		item := recordVariable(scope.record, "")
		scope.local = selector(item)
		index := selector(fmt.Sprintf("datlyWriteIndex%d", l.nextIndex))
		l.nextIndex++
		scope.indexes = append(append([]expression(nil), scope.indexes...), index)
		scope.source = selector(fmt.Sprintf("%s[$%s]", values, index))
		body := block{}
		body.append(assignment{target: index, value: selector("foreach.Index")})
		if err := l.record(&body, scope); err != nil {
			return err
		}
		program.append(forEach{item: item, set: values, body: body})
		return nil
	}
	if scope.record.Cardinality != spec.CardinalityOne {
		return fmt.Errorf("Velty traversal requires explicit cardinality for view %q", scope.record.Identity)
	}
	scope.local = selector(recordVariable(scope.record, ""))
	return l.record(program, scope)
}

func (s recordScope) hook(method string, extra ...expression) (call, error) {
	path, err := inputRecordPath(s.record.InputPath)
	if err != nil {
		return call{}, err
	}
	args := []expression{stringLiteral(path)}
	args = append(args, extra...)
	args = append(args, s.indexes...)
	return call{receiver: selector("writeHooks"), method: method, args: args}, nil
}

func (l *traversalLowerer) record(program *block, scope recordScope) error {
	present, err := scope.hook("Present")
	if err != nil {
		return err
	}
	body := block{}
	if scope.relation != nil {
		for _, link := range scope.relation.Links {
			target, err := selectField(scope.source, link.Child.Field)
			if err != nil {
				return err
			}
			source, err := selectField(scope.parent.source, link.Parent.Field)
			if err != nil {
				return err
			}
			if link.Conversion == plan.LinkDereference {
				body.append(callStatement{call: call{receiver: selector("writeHooks"), method: "Require", args: []expression{source, stringLiteral(link.Parent.Field), stringLiteral(scope.relation.Identity)}}})
			}
			switch link.Conversion {
			case plan.LinkDirect:
				body.append(assignment{target: target, value: source})
			case plan.LinkAddress, plan.LinkDereference:
				child, err := scope.hook("Value")
				if err != nil {
					return err
				}
				parent, err := scope.parent.hook("Value")
				if err != nil {
					return err
				}
				body.append(callStatement{call: call{receiver: selector("writeHooks"), method: "Link", args: []expression{child, parent, stringLiteral(link.Child.Field), stringLiteral(link.Parent.Field)}}})
			default:
				return fmt.Errorf("Velty relation %s has unsupported key conversion %q", scope.relation.Identity, link.Conversion)
			}
			if scope.record.TracksPresence(link.Child.Field) {
				mark, err := scope.hook("Mark", stringLiteral(link.Child.Field))
				if err != nil {
					return err
				}
				body.append(callStatement{call: mark})
			}
		}
	}
	entity, err := scope.hook("Entity")
	if err != nil {
		return err
	}
	body.append(callStatement{call: entity})
	body.append(assignment{target: scope.local, value: scope.source})
	payload, err := scope.hook("Value")
	if err != nil {
		return err
	}
	write, err := lowerWrite(l.operation, scope.record, scope.local, payload)
	if err != nil {
		return err
	}
	body.append(write)
	for _, relation := range scope.record.Relations {
		if relation == nil || relation.Child == nil {
			return fmt.Errorf("Velty traversal received an incomplete relation plan")
		}
		if relation.Child.Auxiliary {
			continue
		}
		if _, err := relation.WriteHookName(); err != nil {
			return err
		}
		hook, err := scope.hook("Relation", stringLiteral(relation.FieldPath[0]))
		if err != nil {
			return err
		}
		body.append(callStatement{call: hook})
		body.append(assignment{target: scope.local, value: scope.source})
		holder, err := selectPath(scope.source, relation.FieldPath)
		if err != nil {
			return err
		}
		child := recordScope{record: relation.Child, source: holder, indexes: scope.indexes, relation: relation, parent: &scope}
		if err = l.traverse(&body, child); err != nil {
			return err
		}
	}
	program.append(conditional{condition: present, thenBlock: body})
	return nil
}
