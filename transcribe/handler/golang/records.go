package golang

import (
	"fmt"
	"go/token"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (l *lowerer) compileRecords() error {
	configured := make(map[string]RecordType, len(l.config.Records))
	for _, binding := range l.config.Records {
		identity := strings.TrimSpace(binding.Identity)
		if identity == "" {
			return fmt.Errorf("generated Go record type binding identity is required")
		}
		path, err := recordPathKey(binding.Path)
		if err != nil {
			return fmt.Errorf("generated Go record type binding %q: %w", identity, err)
		}
		if _, ok := configured[path]; ok {
			return fmt.Errorf("generated Go record type binding path %q is duplicated", path)
		}
		binding.Identity = identity
		binding.Path = append(plan.FieldPath(nil), binding.Path...)
		configured[path] = binding
	}
	if len(configured) == 0 {
		return fmt.Errorf("generated Go record type bindings are required")
	}
	l.recordByPlan = map[*plan.RecordPlan]*recordLowering{}
	l.hookByRelation = map[*plan.RelationPlan]*relationHookLowering{}
	used := map[string]bool{}
	authority := map[string]*recordLowering{}
	hookSignatures := map[string]string{}
	if err := l.compileRecord(l.plan.Root, configured, used, authority, hookSignatures); err != nil {
		return err
	}
	for path, binding := range configured {
		if !used[path] {
			return fmt.Errorf("generated Go record type binding %q at %q is unused", binding.Identity, path)
		}
	}
	return nil
}

func (l *lowerer) compileRecord(record *plan.RecordPlan, configured map[string]RecordType, used map[string]bool, authority map[string]*recordLowering, hookSignatures map[string]string) error {
	if record == nil {
		return fmt.Errorf("generated Go recursive lowering received a nil record plan")
	}
	identity := strings.TrimSpace(record.Identity)
	if identity == "" {
		return fmt.Errorf("generated Go record identity is required at write order %d", record.Write.Order)
	}
	path, err := recordPathKey(record.InputPath)
	if err != nil {
		return fmt.Errorf("generated Go record %q: %w", identity, err)
	}
	if used[path] {
		return fmt.Errorf("generated Go record input path %q occurs more than once", path)
	}
	binding, ok := configured[path]
	if !ok {
		return fmt.Errorf("generated Go record %q at %q has no target type binding", identity, path)
	}
	if binding.Identity != identity {
		return fmt.Errorf("generated Go record path %q is bound to identity %q instead of %q", path, binding.Identity, identity)
	}
	if l.plan.Operation == plan.OperationPatch && !record.Auxiliary && record.Current == nil {
		return fmt.Errorf("Go PATCH record %q requires a planned current input", identity)
	}
	if record.Cardinality != spec.CardinalityOne && record.Cardinality != spec.CardinalityMany {
		return fmt.Errorf("generated Go record %q requires explicit cardinality", identity)
	}
	order := len(l.records)
	if record.Write.Order != order {
		return fmt.Errorf("generated Go record %q has write order %d, expected pre-order %d", identity, record.Write.Order, order)
	}
	if err := validateRecordPolicy(l.plan.Operation, record); err != nil {
		return err
	}
	value, err := parseRecordShape(binding.Value, record.Cardinality)
	if err != nil {
		return fmt.Errorf("record %q value type: %w", identity, err)
	}
	lowered := &recordLowering{plan: record, value: value, order: order}
	if record.Current != nil {
		current, currentErr := parseRecordShape(binding.Current, spec.CardinalityMany)
		if currentErr != nil {
			return fmt.Errorf("record %q current type: %w", identity, currentErr)
		}
		compoundName := l.compoundKeyName(order)
		key, keyErr := l.compileKey(record.Keys, compoundName)
		if keyErr != nil {
			return fmt.Errorf("record %q: %w", identity, keyErr)
		}
		currentKey, keyErr := l.compileKey(record.Current.Keys, compoundName)
		if keyErr != nil {
			return fmt.Errorf("record %q current: %w", identity, keyErr)
		}
		if !sameKeyTypes(key, currentKey) {
			return fmt.Errorf("Go %s record %q and current key types differ", l.plan.Operation, identity)
		}
		lowered.current = current
		lowered.key = key
		if owner := authority[identity]; owner != nil {
			if !sameRecordAuthority(owner, lowered) {
				return fmt.Errorf("generated Go repeated record identity %q has inconsistent types, keys, or current input", identity)
			}
			lowered.owner = owner
			lowered.key = owner.key
		} else {
			lowered.owner = lowered
			authority[identity] = lowered
			for _, name := range []string{l.recordKeyFunction(lowered), l.currentKeyFunction(lowered)} {
				if err = l.reserveDeclaration(name); err != nil {
					return err
				}
			}
			if key.compound {
				if err = l.reserveDeclaration(key.name); err != nil {
					return err
				}
			}
		}
	} else {
		lowered.owner = lowered
	}
	l.records = append(l.records, lowered)
	l.recordByPlan[record] = lowered
	used[path] = true
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil {
			return fmt.Errorf("generated Go record %q contains an incomplete relation", identity)
		}
		if relation.Cardinality != spec.CardinalityOne && relation.Cardinality != spec.CardinalityMany {
			return fmt.Errorf("generated Go relation %q requires explicit cardinality", relation.Identity)
		}
		if relation.Child.Cardinality != relation.Cardinality {
			return fmt.Errorf("generated Go relation %q and child cardinality differ", relation.Identity)
		}
		expectedPath := append(append(plan.FieldPath(nil), record.InputPath...), relation.FieldPath...)
		if !samePath(expectedPath, relation.Child.InputPath) {
			return fmt.Errorf("generated Go relation %q child input path is not rooted at its parent", relation.Identity)
		}
		if len(relation.Links) == 0 {
			return fmt.Errorf("generated Go relation %q requires a planned key link", relation.Identity)
		}
		for _, link := range relation.Links {
			for _, field := range []string{link.Parent.Field, link.Child.Field} {
				if !token.IsIdentifier(strings.TrimSpace(field)) || token.Lookup(strings.TrimSpace(field)).IsKeyword() {
					return fmt.Errorf("generated Go relation %q link field %q is not a Go identifier", relation.Identity, field)
				}
			}
		}
		if err = l.compileRecord(relation.Child, configured, used, authority, hookSignatures); err != nil {
			return err
		}
		child := l.recordByPlan[relation.Child]
		if child == nil {
			return fmt.Errorf("generated Go relation %q has no compiled child record", relation.Identity)
		}
		if !child.plan.Auxiliary {
			if err = l.compileRelationHook(lowered, relation, child, hookSignatures); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *lowerer) compileRelationHook(parent *recordLowering, relation *plan.RelationPlan, child *recordLowering, signatures map[string]string) error {
	method, err := relation.WriteHookName()
	if err != nil {
		return err
	}
	holderType := child.value.expression()
	signatureKey := parent.value.base + "." + method
	if previous := signatures[signatureKey]; previous != "" && previous != holderType {
		return fmt.Errorf("generated Go relation hook %s has conflicting holder types %s and %s", signatureKey, previous, holderType)
	}
	signatures[signatureKey] = holderType
	name := fmt.Sprintf("%sRelation%dWriter", l.handler, len(l.relationHooks))
	if err := l.reserveDeclaration(name); err != nil {
		return err
	}
	hook := &relationHookLowering{
		interfaceName: name, methodName: method,
		holderType: holderType, order: len(l.relationHooks),
	}
	l.relationHooks = append(l.relationHooks, hook)
	l.hookByRelation[relation] = hook
	return nil
}

func recordPathKey(path plan.FieldPath) (string, error) {
	if len(path) < 2 || path[0] != "Input" {
		return "", fmt.Errorf("record input path must be rooted at Input")
	}
	for _, field := range path[1:] {
		if !token.IsIdentifier(strings.TrimSpace(field)) || token.Lookup(strings.TrimSpace(field)).IsKeyword() {
			return "", fmt.Errorf("record input path contains invalid field %q", field)
		}
	}
	return strings.Join(path, "."), nil
}

func sameRecordAuthority(left, right *recordLowering) bool {
	if left == nil || right == nil || left.value.base != right.value.base || left.value.pointer != right.value.pointer ||
		left.current.base != right.current.base || left.current.pointer != right.current.pointer ||
		!samePath(left.plan.Current.InputPath, right.plan.Current.InputPath) || !sameKeyTypes(left.key, right.key) {
		return false
	}
	return sameKeyParts(left.plan.Keys, right.plan.Keys) && sameKeyParts(left.plan.Current.Keys, right.plan.Current.Keys)
}

func sameKeyParts(left, right []plan.KeyPart) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Field != right[index].Field || left[index].Type != right[index].Type {
			return false
		}
	}
	return true
}

func validateRecordPolicy(operation plan.Operation, record *plan.RecordPlan) error {
	if !samePath(record.Write.ValuePath, record.InputPath) {
		return fmt.Errorf("generated Go record %q write value path does not match its input path", record.Identity)
	}
	if record.Auxiliary {
		if record.Sequence != nil || record.Write.Existing != "" || record.Write.Missing != "" || len(record.Write.Allowed) != 0 {
			return fmt.Errorf("auxiliary record %q has mutation operations", record.Identity)
		}
		return nil
	}
	switch operation {
	case plan.OperationPost:
		if record.Write.Existing != "" || record.Write.Missing != plan.ActionInsert ||
			len(record.Write.Allowed) != 1 || !containsAction(record.Write.Allowed, plan.ActionInsert) {
			return fmt.Errorf("generated Go POST record %q requires insert-only write policy", record.Identity)
		}
		return nil
	case plan.OperationPut:
		if record.Write.Missing != "" || record.Write.Existing != plan.ActionUpdate ||
			len(record.Write.Allowed) != 1 || !containsAction(record.Write.Allowed, plan.ActionUpdate) {
			return fmt.Errorf("generated Go PUT record %q requires update-only write policy", record.Identity)
		}
		return nil
	case plan.OperationPatch:
	default:
		return fmt.Errorf("unsupported generated Go write operation %q", operation)
	}
	if record.Current == nil {
		return fmt.Errorf("Go PATCH record %q requires a planned current input", record.Identity)
	}
	if len(record.Current.Keys) != len(record.Keys) {
		return fmt.Errorf("Go PATCH record %q and current key arity differ: %d and %d", record.Identity, len(record.Keys), len(record.Current.Keys))
	}
	for _, action := range []plan.Action{record.Write.Existing, record.Write.Missing} {
		if !containsAction(record.Write.Allowed, action) {
			return fmt.Errorf("Go PATCH record %q action %q is not allowed by the semantic write policy", record.Identity, action)
		}
	}
	if record.Write.Existing != plan.ActionUpdate || record.Write.Missing != plan.ActionInsert || len(record.Write.Allowed) != 2 {
		return fmt.Errorf("Go PATCH record %q requires update-or-insert write policy", record.Identity)
	}
	return nil
}

func sameKeyTypes(left, right keyShape) bool {
	if len(left.types) != len(right.types) {
		return false
	}
	for index := range left.types {
		if left.types[index] != right.types[index] {
			return false
		}
	}
	return true
}

func (l *lowerer) compoundKeyName(order int) string {
	if order == 0 {
		return l.handler + "Key"
	}
	return fmt.Sprintf("%sRelation%dKey", l.handler, order)
}

func (l *lowerer) recordKeyFunction(record *recordLowering) string {
	record = record.owner
	if record.order == 0 {
		return l.handler + "RecordKey"
	}
	return fmt.Sprintf("%sRecord%dKey", l.handler, record.order)
}

func (l *lowerer) currentKeyFunction(record *recordLowering) string {
	record = record.owner
	if record.order == 0 {
		return l.handler + "CurrentKey"
	}
	return fmt.Sprintf("%sCurrent%dKey", l.handler, record.order)
}

func (l *lowerer) currentIndexVariable(record *recordLowering) string {
	record = record.owner
	if record.order == 0 {
		return "currentByKey"
	}
	return fmt.Sprintf("current%dByKey", record.order)
}

func (l *lowerer) usesSequencer() bool {
	for _, record := range l.records {
		if record.plan.Sequence != nil {
			return true
		}
	}
	return false
}
