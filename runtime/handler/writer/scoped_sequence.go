package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/sqlx/io/sequence"
	xhandler "github.com/viant/xdatly/handler"
)

type ScopedSequence struct {
	Field Field
	Scope []Field
}

type scopedSequencer interface {
	ReserveScoped(context.Context, string, string, []sequence.Scope, int, []int64) ([]int64, error)
}

func compileScopedSequences(record *Record) error {
	for _, field := range record.Fields {
		structField := record.EntityType.FieldByIndex(field.Index)
		declaration := structField.Tag.Get("sequenceScope")
		if declaration == "" {
			continue
		}
		if record.Auxiliary || field.Column == "" || field.Column == "-" || !numericField(record.EntityType, field) {
			return fmt.Errorf("sequence_scope %s requires a writable integer column", field.Name)
		}
		for _, key := range record.Keys {
			if key.Name == field.Name {
				return fmt.Errorf("sequence_scope is for non-identity columns")
			}
		}
		if record.ConcurrencyToken != nil && record.ConcurrencyToken.Name == field.Name {
			return fmt.Errorf("sequence_scope cannot target a concurrency token")
		}
		plan := ScopedSequence{Field: field}
		seen := map[string]bool{}
		for _, name := range strings.Split(declaration, ",") {
			name = strings.TrimSpace(name)
			var matched *Field
			for i := range record.Fields {
				candidate := &record.Fields[i]
				if strings.EqualFold(candidate.Column, name) || candidate.Name == name {
					if matched != nil {
						return fmt.Errorf("ambiguous sequence_scope field %s", name)
					}
					matched = candidate
				}
			}
			if matched == nil || matched.Column == "" || matched.Column == "-" || matched.Name == field.Name || seen[matched.Name] {
				return fmt.Errorf("invalid sequence_scope field %s", name)
			}
			kind := dereference(record.EntityType.FieldByIndex(matched.Index).Type).Kind()
			if kind != reflect.String && kind != reflect.Bool && (kind < reflect.Int || kind > reflect.Uint64) {
				return fmt.Errorf("sequence_scope %s must be string, boolean or integer", name)
			}
			seen[matched.Name] = true
			plan.Scope = append(plan.Scope, *matched)
		}
		record.ScopedSequences = append(record.ScopedSequences, plan)
	}
	return nil
}

type scopedTarget struct {
	frame *Frame
	field Field
}

type scopedGroup struct {
	table, column string
	scope         []sequence.Scope
	supplied      []int64
	targets       []scopedTarget
	field         Field
}

func (p *Program) allocateScoped(ctx context.Context, capability xhandler.Sequencer) error {
	groups := map[string]*scopedGroup{}
	for _, frame := range p.frames.Rows {
		if frame == nil || frame.SkippedDelete || frame.Record.Auxiliary || frame.Action == xhandler.WriteDelete {
			continue
		}
		for _, plan := range frame.Record.ScopedSequences {
			scope := []sequence.Scope{}
			resolved := true
			for _, field := range plan.Scope {
				value := frame.Entity.Elem().FieldByIndex(field.Index)
				for value.Kind() == reflect.Pointer {
					if value.IsNil() {
						resolved = false
						break
					}
					value = value.Elem()
				}
				if !resolved || value.Kind() == reflect.String && value.String() == "" {
					resolved = false
					break
				}
				var scalar any
				switch value.Kind() {
				case reflect.String:
					scalar = value.String()
				case reflect.Bool:
					scalar = value.Bool()
				default:
					if value.Kind() >= reflect.Int && value.Kind() <= reflect.Int64 {
						scalar = value.Int()
					} else {
						scalar = value.Uint()
					}
				}
				scope = append(scope, sequence.Scope{Column: field.Column, Value: scalar})
			}
			if !resolved {
				continue
			} // An empty/nil turn preserves the unsequenced value.
			encoded, err := json.Marshal([]any{frame.Record.Table, plan.Field.Column, scope})
			if err != nil {
				return err
			}
			key := string(encoded)
			group := groups[key]
			if group == nil {
				group = &scopedGroup{table: frame.Record.Table, column: plan.Field.Column, scope: scope, field: plan.Field}
				groups[key] = group
			}
			value := frame.Entity.Elem().FieldByIndex(plan.Field.Index)
			supplied := frame.Original != nil && frame.Original.Has(plan.Field.Name)
			if supplied {
				if n, valid, err := scopedInteger(value); err != nil {
					return err
				} else if valid {
					group.supplied = append(group.supplied, n)
				}
				continue
			}
			if frame.Action != xhandler.WriteInsert {
				continue
			}
			// A business hook can supply a value; automatic allocation does not erase it.
			if n, valid, err := scopedInteger(value); err != nil {
				return err
			} else if valid && n != 0 {
				group.supplied = append(group.supplied, n)
				continue
			}
			group.targets = append(group.targets, scopedTarget{frame: frame, field: plan.Field})
		}
	}
	if len(groups) == 0 {
		return nil
	}
	p.scopedService = capability
	service, ok := capability.(scopedSequencer)
	if !ok {
		return fmt.Errorf("native scoped sequencer capability is required")
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		if len(group.targets) == 0 {
			continue
		}
		values, err := service.ReserveScoped(ctx, group.table, group.column, group.scope, len(group.targets), group.supplied)
		if err != nil {
			return fmt.Errorf("sequence_scope %s.%s: %w", group.table, group.column, err)
		}
		if len(values) != len(group.targets) {
			return fmt.Errorf("scoped sequencer returned an invalid count")
		}
		// Validate every destination before mutating any of this partition's fields.
		for i, target := range group.targets {
			frame := target.frame
			if err := setScopedInteger(frame.Entity.Elem().FieldByIndex(target.field.Index), values[i], false); err != nil {
				return err
			}
		}
		for i, target := range group.targets {
			frame := target.frame
			if err := setScopedInteger(frame.Entity.Elem().FieldByIndex(target.field.Index), values[i], true); err != nil {
				return err
			}
			if frame.ScopedAllocated == nil {
				frame.ScopedAllocated = map[string]bool{}
			}
			frame.ScopedAllocated[target.field.Name] = true
			markSupplied(frame.Entity.Elem(), target.field)
			frame.Fields.force(target.field.Name)
		}
	}
	return nil
}
func scopedInteger(value reflect.Value) (int64, bool, error) {
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, false, nil
		}
		value = value.Elem()
	}
	if value.Kind() >= reflect.Int && value.Kind() <= reflect.Int64 {
		return value.Int(), true, nil
	}
	if value.Kind() >= reflect.Uint && value.Kind() <= reflect.Uint64 {
		n := value.Uint()
		if n > uint64(^uint64(0)>>1) {
			return 0, false, fmt.Errorf("scoped sequence exceeds int64")
		}
		return int64(n), true, nil
	}
	return 0, false, fmt.Errorf("scoped sequence must be integer")
}
func setScopedInteger(value reflect.Value, n int64, assign bool) error {
	typ := value.Type()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	check := reflect.New(typ).Elem()
	if typ.Kind() >= reflect.Int && typ.Kind() <= reflect.Int64 {
		if check.OverflowInt(n) {
			return fmt.Errorf("scoped sequence overflows %s", typ)
		}
		check.SetInt(n)
	} else {
		if n < 0 || check.OverflowUint(uint64(n)) {
			return fmt.Errorf("scoped sequence overflows %s", typ)
		}
		check.SetUint(uint64(n))
	}
	if !assign {
		return nil
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}
	value.Set(check)
	return nil
}
