package writer

import (
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

// Internal preparation for the reviewed source-phase mode. No public descriptor
// compiles this mode until complete phase/allocation/payload authority is ready.
type finiteRootDecisionMetadata struct {
	key     Field
	readKey func(unsafe.Pointer) int64
}
type finiteRootOccurrence struct {
	row                 reflect.Value
	key                 int64
	supplied, available bool
}
type finiteRootDecision struct {
	action      xhandler.WriteAction
	metadata    *finiteRootDecisionMetadata
	occurrences []finiteRootOccurrence
}

func compileFiniteRootDecision(root *Record) (*finiteRootDecisionMetadata, error) {
	if root == nil || len(root.Keys) != 1 || root.Auxiliary || root.Sequence == nil {
		return nil, fmt.Errorf("finite root decision requires one physical autoincrement key")
	}
	key := root.Keys[0]
	if !root.Sequence.AutoIncrement || !sameIdentityField(key, *root.Sequence) || len(key.Index) != 1 || len(key.Has) == 0 {
		return nil, fmt.Errorf("finite root decision requires a direct marked autoincrement key")
	}
	field := root.EntityType.FieldByIndex(key.Index)
	accessor := xunsafe.NewField(field)
	result := &finiteRootDecisionMetadata{key: key}
	switch field.Type.Kind() {
	case reflect.Int:
		result.readKey = func(p unsafe.Pointer) int64 { return int64(accessor.Int(p)) }
	case reflect.Int64:
		result.readKey = accessor.Int64
	case reflect.Int32:
		result.readKey = func(p unsafe.Pointer) int64 { return int64(accessor.Int32(p)) }
	case reflect.Int16:
		result.readKey = func(p unsafe.Pointer) int64 { return int64(accessor.Int16(p)) }
	case reflect.Int8:
		result.readKey = func(p unsafe.Pointer) int64 { return int64(accessor.Int8(p)) }
	default:
		return nil, fmt.Errorf("finite root decision requires a signed integer key")
	}
	return result, nil
}

func (p *Program) captureFiniteRootDecision(rows reflect.Value) error {
	root := p.metadata.Root
	if root == nil || root.reconciliation == nil || root.reconciliation.rootDecision == nil {
		return nil
	}
	if rows.Kind() != reflect.Slice {
		return fmt.Errorf("finite root decision requires a root collection")
	}
	decision := &finiteRootDecision{action: xhandler.WriteInsert, metadata: root.reconciliation.rootDecision}
	update := rows.Len() > 0
	for n := 0; n < rows.Len(); n++ {
		row := rows.Index(n)
		occurrence := finiteRootOccurrence{row: reflect.ValueOf(row.Interface())}
		if row.IsNil() {
			update = false
		} else {
			occurrence.key = decision.metadata.readKey(row.UnsafePointer())
			occurrence.available = presenceAvailable(row.Elem())
			occurrence.supplied = supplied(row.Elem(), decision.metadata.key)
			update = update && occurrence.available && occurrence.supplied && occurrence.key > 0
		}
		decision.occurrences = append(decision.occurrences, occurrence)
	}
	if update {
		decision.action = xhandler.WriteUpdate
	}
	p.finiteRootDecision = decision
	// A nil remains INSERT evidence, but skipping nil physical roots is not yet
	// established for this mode. Do not drop it and misclassify the survivors.
	for _, occurrence := range decision.occurrences {
		if occurrence.row.IsNil() {
			return fmt.Errorf("finite root decision null occurrence is not supported")
		}
	}
	return nil
}

// This preallocation seal is independent of the pointer-keyed Original map:
// duplicate pointers still have distinct captured occurrence positions.
func (p *Program) validateFiniteRootDecision(rows reflect.Value) error {
	decision := p.finiteRootDecision
	if decision == nil {
		return nil
	}
	if rows.Kind() != reflect.Slice || rows.Len() != len(decision.occurrences) {
		return fmt.Errorf("finite root decision changed root population")
	}
	for n, occurrence := range decision.occurrences {
		row := rows.Index(n)
		if row.IsNil() || row.Pointer() != occurrence.row.Pointer() {
			return fmt.Errorf("finite root decision changed occurrence %d", n)
		}
		if decision.metadata.readKey(row.UnsafePointer()) != occurrence.key || presenceAvailable(row.Elem()) != occurrence.available || supplied(row.Elem(), decision.metadata.key) != occurrence.supplied {
			return fmt.Errorf("finite root decision changed original key facts at occurrence %d", n)
		}
	}
	return nil
}

func (p *Program) isFiniteRootInsert(frame *Frame) bool {
	return p.finiteRootDecision != nil && frame != nil && frame.Parent == nil && frame.Record == p.metadata.Root && frame.Action == xhandler.WriteInsert
}
