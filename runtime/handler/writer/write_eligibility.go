package writer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
)

func validateWriteEligibilityHooks(metadata *Metadata, outputType reflect.Type) error {
	root := metadata.Root
	var visit func(*Record) error
	seen := map[*Record]bool{}
	visit = func(record *Record) error {
		if record == nil || seen[record] {
			return nil
		}
		seen[record] = true
		if record.HookType != nil {
			method := reflect.New(record.HookType).MethodByName("WriteEligible")
			if method.IsValid() {
				if record != root || record.Auxiliary || record.DeleteMarker != nil || (metadata.Operation != "patch" && metadata.Operation != "post" && metadata.Operation != "put") {
					return fmt.Errorf("WriteEligible requires an INSERT/UPDATE physical root without deletion")
				}
				typ := method.Type()
				if typ.IsVariadic() || typ.NumIn() != 4 || typ.NumOut() != 2 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(record.EntityType) || typ.In(3) != reflect.TypeFor[h.WriteAction]() || typ.Out(0) != reflect.TypeFor[bool]() || typ.Out(1) != reflect.TypeFor[error]() {
					return fmt.Errorf("WriteEligible requires canonical entity, LifecycleContext, WriteAction and (bool, error)")
				}
				state := typ.In(2)
				if state.Kind() != reflect.Struct || state.PkgPath() != reflect.TypeFor[h.NoParent]().PkgPath() || !strings.HasPrefix(state.Name(), "LifecycleContext[") {
					return fmt.Errorf("WriteEligible requires canonical LifecycleContext")
				}
				previous, prevOK := state.FieldByName("Previous")
				parent, parentOK := state.FieldByName("Parent")
				output, outOK := state.FieldByName("Output")
				if !prevOK || previous.Type != reflect.PointerTo(record.EntityType) || !parentOK || parent.Type != reflect.TypeFor[*h.NoParent]() || !outOK || output.Type != reflect.PointerTo(outputType) {
					return fmt.Errorf("WriteEligible requires root LifecycleContext entity and output types")
				}
				record.writeEligibility = true
			}
		}
		for _, relation := range record.Relations {
			if root.writeEligibility && !relation.Child.Auxiliary {
				if record != root || relation.Child == root || len(relation.Child.Relations) != 0 || len(relation.Links) == 0 {
					return fmt.Errorf("WriteEligible supports only linked direct writable leaf children")
				}
			}
			if err := visit(relation.Child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

// evaluateWriteEligibility protects the whole callback pass. Validation and
// allocation use the original frames; only queue membership is filtered later.
func (p *Program) evaluateWriteEligibility(ctx context.Context) (err error) {
	if !p.metadata.Root.writeEligibility {
		return nil
	}
	finish, err := engine.BeginWriteEligibility(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, finish()) }()
	return p.decideWriteEligibility(ctx)
}

func (p *Program) eligibilityState() (string, error) {
	values := []reflect.Value{reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField), reflect.ValueOf(p.output)}
	for _, frame := range p.frames.Rows {
		if frame != nil {
			values = append(values, frame.Entity, frame.Previous)
			if frame.Previous.IsValid() && frame.Previous.Kind() == reflect.Pointer && !frame.Previous.IsNil() {
				values = append(values, reflect.ValueOf(p.fieldsOf(frame.Previous.Elem().Type())))
			}
			if frame.Original != nil {
				values = append(values, reflect.ValueOf(frame.Original.Available()))
				for _, field := range frame.Record.Fields {
					values = append(values, reflect.ValueOf(frame.Original.Has(field.Name)))
				}
			}
		}
	}
	return immutableValues(values)
}

func (p *Program) decideWriteEligibility(ctx context.Context) (err error) {
	before, err := p.eligibilityState()
	if err != nil {
		return err
	}
	defer func() {
		after, snapshotErr := p.eligibilityState()
		if snapshotErr != nil {
			err = errors.Join(err, snapshotErr)
		} else if before != after {
			err = errors.Join(err, errors.New("WriteEligible changed writer body, presence, Previous, output or associations"))
		}
		p.graph = nil
	}()
	for _, frame := range p.frames.Rows {
		if frame == nil || frame.Record != p.metadata.Root || (frame.Action != h.WriteInsert && frame.Action != h.WriteUpdate) {
			continue
		}
		frame.ExcludedWrite = false
		if err = ctx.Err(); err != nil {
			return err
		}
		if !frame.Hook.IsValid() {
			return errors.New("WriteEligible lifecycle is unavailable")
		}
		method := frame.Hook.MethodByName("WriteEligible")
		state := p.entityHookState(method.Type().In(2), frame)
		result := method.Call([]reflect.Value{reflect.ValueOf(ctx), frame.Entity, state, reflect.ValueOf(frame.Action)})
		var callbackErr error
		if !result[1].IsNil() {
			callbackErr = result[1].Interface().(error)
		}
		after, snapshotErr := p.eligibilityState()
		if snapshotErr == nil && after != before {
			snapshotErr = errors.New("WriteEligible changed writer body, presence, Previous, output or associations")
		}
		if combined := errors.Join(callbackErr, snapshotErr); combined != nil {
			return combined
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if !result[0].Bool() {
			if err := p.validateExcludedRootProducer(frame); err != nil {
				return err
			}
		}
		frame.ExcludedWrite = !result[0].Bool()
	}
	return nil
}

// An excluded parent may keep child writes only when its persisted relation
// producers already exist unchanged. Checking metadata also covers empty slices.
func (p *Program) validateExcludedRootProducer(frame *Frame) error {
	for _, relation := range frame.Record.Relations {
		if relation.Child.Auxiliary {
			continue
		}
		if frame.Action != h.WriteUpdate || !frame.Previous.IsValid() || frame.Previous.Kind() != reflect.Pointer || frame.Previous.IsNil() {
			return errors.New("WriteEligible cannot suppress an INSERT root with writable children")
		}
		for _, link := range relation.Links {
			field := &link.Parent
			if !p.previousFields[frame.Record].Has(field.Name) {
				return fmt.Errorf("WriteEligible requires loaded Previous for child producer %s", field.Name)
			}
			prior := previousField(frame.Previous.Elem(), frame.Record, field)
			current := frame.Entity.Elem().FieldByIndex(field.Index)
			if !prior.IsValid() || !concurrencyTokenEqual(current.Interface(), prior.Interface()) {
				return fmt.Errorf("WriteEligible cannot suppress changed child producer %s", field.Name)
			}
		}
	}
	return nil
}

func (p *Program) filterIneligibleActions() {
	if !p.metadata.Root.writeEligibility {
		return
	}
	keep := func(actions []*Action) []*Action {
		result := actions[:0]
		for _, action := range actions {
			frame := p.actionFrame(action)
			if frame == nil || !frame.ExcludedWrite {
				result = append(result, action)
			}
		}
		return result
	}
	p.actions.Rows = keep(p.actions.Rows)
	p.queueItems = keep(p.queueItems)
}

// immutableValues records values and object identities, including nil versus
// empty collections and unexported scalar fields. Cycles and aliases retain
// their association identity. It never marshals JSON or exposes captured data.
func immutableValues(values []reflect.Value) (string, error) {
	var result strings.Builder
	type identity struct {
		typ      reflect.Type
		ptr      uintptr
		length   int
		capacity int
	}
	seen := map[identity]bool{}
	var walk func(reflect.Value) error
	walk = func(v reflect.Value) error {
		if !v.IsValid() {
			result.WriteString("invalid;")
			return nil
		}
		result.WriteString(v.Type().PkgPath() + ":" + v.Type().String() + "{")
		defer result.WriteByte('}')
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				result.WriteString("nil")
				return nil
			}
			return walk(v.Elem())
		case reflect.Pointer, reflect.Map, reflect.Slice:
			if v.IsNil() {
				result.WriteString("nil")
				return nil
			}
			var ptr uintptr
			if v.Kind() == reflect.Map {
				ptr = uintptr(v.UnsafePointer())
			} else {
				ptr = v.Pointer()
			}
			result.WriteString(strconv.FormatUint(uint64(ptr), 16) + ";")
			if v.Kind() == reflect.Slice {
				result.WriteString(fmt.Sprintf("%d/%d;", v.Len(), v.Cap()))
			}
			key := identity{typ: v.Type(), ptr: ptr}
			if v.Kind() == reflect.Slice {
				key.length = v.Len()
				key.capacity = v.Cap()
			}
			if seen[key] {
				return nil
			}
			seen[key] = true
			if v.Kind() == reflect.Pointer {
				return walk(v.Elem())
			}
			if v.Kind() == reflect.Map {
				type entry struct {
					key      string
					keyValue reflect.Value
					value    reflect.Value
				}
				var pairs []entry
				it := v.MapRange()
				for it.Next() {
					keyText, keyErr := immutableMapKey(it.Key())
					if keyErr != nil {
						return keyErr
					}
					pairs = append(pairs, entry{keyText, it.Key(), it.Value()})
				}
				sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
				for i, pair := range pairs {
					if i > 0 && pairs[i-1].key == pair.key {
						return errors.New("WriteEligible cannot verify ambiguous map key encodings")
					}
					result.WriteString(pair.key)
					if err := walk(pair.keyValue); err != nil {
						return err
					}
					if err := walk(pair.value); err != nil {
						return err
					}
				}
				return nil
			}
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				// A nested component output can carry its invocation logger.
				// Its mutable runtime internals are not writer contract data.
				// Keep ordinary function-bearing data fail-closed below.
				if slices.Contains(strings.Split(v.Type().Field(i).Tag.Get("parameter"), ","), "kind=logger") {
					continue
				}
				if err := walk(v.Field(i)); err != nil {
					return fmt.Errorf("%s.%s: %w", v.Type(), v.Type().Field(i).Name, err)
				}
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i)); err != nil {
					return err
				}
			}
		case reflect.String:
			result.WriteString(strconv.Quote(v.String()))
		case reflect.Bool:
			result.WriteString(strconv.FormatBool(v.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			result.WriteString(strconv.FormatInt(v.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			result.WriteString(strconv.FormatUint(v.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			result.WriteString(strconv.FormatUint(math.Float64bits(v.Float()), 16))
		case reflect.Complex64, reflect.Complex128:
			c := v.Complex()
			result.WriteString(fmt.Sprintf("%x/%x", math.Float64bits(real(c)), math.Float64bits(imag(c))))
		default:
			return fmt.Errorf("WriteEligible cannot verify immutable value of kind %s", v.Kind())
		}
		return nil
	}
	for _, v := range values {
		if err := walk(v); err != nil {
			return "", err
		}
	}
	return result.String(), nil
}

// Map keys compare pointer identities, not the objects they point at. This also
// avoids recursively entering a map through a pointer key back to that map.
func immutableMapKey(v reflect.Value) (string, error) {
	if v.Kind() == reflect.Pointer || v.Kind() == reflect.Chan || v.Kind() == reflect.UnsafePointer {
		return fmt.Sprintf("%s:%x", v.Type(), v.Pointer()), nil
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return "nil", nil
		}
		return immutableMapKey(v.Elem())
	}
	if v.Kind() == reflect.Struct || v.Kind() == reflect.Array {
		var parts strings.Builder
		parts.WriteString(v.Type().String() + "{")
		size := v.Len
		element := v.Index
		if v.Kind() == reflect.Struct {
			size = v.NumField
			element = v.Field
		}
		for i := 0; i < size(); i++ {
			value, err := immutableMapKey(element(i))
			if err != nil {
				return "", err
			}
			parts.WriteString(value + ";")
		}
		parts.WriteByte('}')
		return parts.String(), nil
	}
	return immutableValues([]reflect.Value{v})
}
