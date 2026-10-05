package writer

import (
	"context"
	"github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx/io/errx"
	"github.com/viant/sqlx/io/sequence"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

func hasScopedSequences(record *Record) bool {
	if record == nil {
		return false
	}
	if len(record.ScopedSequences) > 0 {
		return true
	}
	for _, r := range record.Relations {
		if hasScopedSequences(r.Child) {
			return true
		}
	}
	return false
}
func (h *Handler) ScopedMutationRetryLimit() int { return 9 } // Initial attempt plus at most nine replays.
func (h *Handler) RecoverScopedMutation(ctx context.Context, invocation rhandler.Invocation, report exec.MutationReport, outcome xhandler.Outcome) (bool, error) {
	if h == nil || !h.scopedSequences || report.Nested || len(outcome.Transactions) != 1 || outcome.Transactions[0].State != xhandler.TransactionRolledBack {
		return false, nil
	}
	// An early writer transaction can encounter a coded serialization/lock
	// failure during binding or allocation, before any row reaches the queue.
	// Its confirmed rollback permits the same bounded native replay policy.
	if report.Contention {
		return true, nil
	}
	duplicate := false
	for _, result := range report.Results {
		if result.Error != nil {
			if !errx.IsDuplicateKey(result.Error) {
				return false, nil
			}
			duplicate = true
		}
	}
	if !duplicate {
		return false, nil
	}
	program, _ := invocation.Snapshot.(*Program)
	if program == nil {
		return false, nil
	}
	checker, ok := program.scopedService.(interface {
		ScopedSequenceCollision(context.Context, string, string, []sequence.Scope, int64, []sequence.Scope) (bool, error)
	})
	if !ok {
		return false, nil
	}
	matched := false
	for _, frame := range program.frames.Rows {
		if frame.Action != xhandler.WriteInsert || frame.Record.Auxiliary {
			continue
		}
		for _, plan := range frame.Record.ScopedSequences {
			if !frame.ScopedAllocated[plan.Field.Name] {
				continue
			}
			value, valid, e := scopedInteger(frame.Entity.Elem().FieldByIndex(plan.Field.Index))
			if e != nil {
				return false, e
			}
			if !valid {
				continue
			}
			scope := []sequence.Scope{}
			identity := []sequence.Scope{}
			for _, field := range plan.Scope {
				v := frame.Entity.Elem().FieldByIndex(field.Index)
				for v.Kind() == reflect.Pointer {
					if v.IsNil() {
						return false, nil
					}
					v = v.Elem()
				}
				scope = append(scope, sequence.Scope{Column: field.Column, Value: v.Interface()})
			}
			for _, field := range frame.Record.Keys {
				v := frame.Entity.Elem().FieldByIndex(field.Index)
				for v.Kind() == reflect.Pointer {
					if v.IsNil() {
						return false, nil
					}
					v = v.Elem()
				}
				identity = append(identity, sequence.Scope{Column: field.Column, Value: v.Interface()})
			}
			collision, e := checker.ScopedSequenceCollision(ctx, frame.Record.Table, plan.Field.Column, scope, value, identity)
			if e != nil {
				return false, e
			}
			matched = matched || collision
		}
	}
	return matched, nil
}

type scopedReplayKey struct{}

func (h *Handler) MutationReplayContext(ctx context.Context, invocation rhandler.Invocation) context.Context {
	if h == nil || !h.scopedSequences {
		return ctx
	}
	mustInsert := map[rowIdentity]bool{}
	if previous, ok := ctx.Value(scopedReplayKey{}).(map[rowIdentity]bool); ok {
		for key, value := range previous {
			mustInsert[key] = value
		}
	}
	program, _ := invocation.Snapshot.(*Program)
	if program != nil {
		for _, frame := range program.frames.Rows {
			if frame.Action == xhandler.WriteInsert {
				key, complete := frame.Record.key(frame.Entity.Elem())
				if complete {
					mustInsert[rowIdentity{record: frame.Record, key: key}] = true
				}
			}
		}
	}
	return context.WithValue(ctx, scopedReplayKey{}, mustInsert)
}
