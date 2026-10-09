package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
)

// Current-based values retain their canonical occurrence context. Only an
// already-admitted primary root INSERT can satisfy a buffered FK dependency.
// Unreached canonical INSERT frames are not producer receipts.
func (p *Program) finiteCurrentValidationOptions(canonical, effective *Frame) (h.ValidationOptions, error) {
	if canonical == nil || effective == nil || effective.Record != canonical.Record || effective.Parent != canonical.Parent || effective.Location != canonical.Location || effective.Action != h.WriteUpdate || !effective.Previous.IsValid() || !effective.Entity.IsValid() || effective.Entity.Type() != canonical.Entity.Type() {
		return h.ValidationOptions{}, fmt.Errorf("source Current validation lost canonical context")
	}
	if canonical.Original == nil || effective.Original == nil || canonical.Original.Available() != effective.Original.Available() {
		return h.ValidationOptions{}, fmt.Errorf("source Current validation lost Original presence")
	}
	for _, field := range canonical.Record.Fields {
		if canonical.Original.Has(field.Name) != effective.Original.Has(field.Name) {
			return h.ValidationOptions{}, fmt.Errorf("source Current validation changed Original presence")
		}
	}
	index := newGraphIndex(p.frames.Rows) // allocation may have changed cached keys
	position, found := index.positions[canonical]
	if !found {
		return h.ValidationOptions{}, fmt.Errorf("source Current validation has no canonical graph position")
	}
	var refs []h.ValidationReference
	a := p.reconciliation
	if a != nil && a.rootAdmitted && a.rootAppendPrefix == len(a.roots) {
		for _, field := range effective.Record.Fields {
			for _, producer := range index.producers(field, effective.Entity.Elem().FieldByIndex(field.Index)) {
				if producer.position >= position || producer.frame == canonical || producer.frame.Parent != nil || producer.frame.Record != p.metadata.Root {
					continue
				}
				for i, root := range a.roots {
					if i >= len(p.rootAdmissionSpan) {
						return h.ValidationOptions{}, fmt.Errorf("source Current validation lost root admission prefix")
					}
					action := p.rootAdmissionSpan[i]
					if root.frame == producer.frame && action != nil && action.frame == root.frame && action.Kind == h.WriteInsert {
						refs = append(refs, h.ValidationReference{Field: field.Name, Schema: field.RefDB, Table: field.RefTable, Column: field.RefColumn})
						break
					}
				}
			}
		}
	}
	effective.Fields = livePresence(effective.Record, effective.Entity)
	return p.validationOptionsWithReferences(effective, true, refs), nil
}

// This read-only prerequisite does not apply setters to cumulative Current,
// append a child action, advance a cursor or grant completion authority.
func (p *Program) validateFinitePendingCurrent(ctx context.Context, pending *finitePendingUpdate) (err error) {
	var finish func() error
	defer func() {
		v := recover()
		if finish != nil {
			func() {
				defer func() {
					if nested := recover(); nested != nil {
						if v == nil {
							v = nested
						}
						err = errors.Join(err, fmt.Errorf("source Current validation guard panicked"))
					}
				}()
				err = errors.Join(err, p.validateFinitePhaseTransition(ctx, pending.transition), p.validateFiniteRetainedActions())
			}()
		}
		if finish != nil {
			err = errors.Join(err, finish())
		}
		err = errors.Join(err, ctx.Err())
		if v != nil {
			err = errors.Join(err, fmt.Errorf("source Current validation panicked"))
		}
		if err != nil {
			p.retireFiniteCursor(err)
		}
		if v != nil {
			panic(v)
		}
	}()
	if pending == nil || pending != p.finitePendingUpdate {
		return fmt.Errorf("source Current validation requires its exact pending image")
	}
	if err = p.validateFinitePhaseTransition(ctx, pending.transition); err != nil {
		return err
	}
	if err = p.validateFiniteRetainedActions(); err != nil {
		return err
	}
	service, e := lookup[h.DML](ctx, p.guardBinder, h.DMLKey)
	if e != nil {
		return e
	}
	if err = engine.ValidateBoundDML(ctx, service, p.guardBinding); err != nil {
		return err
	}
	guarded, ok := service.(interface{ ValidateExecutionGuards(context.Context) error })
	if !ok {
		return fmt.Errorf("source Current validation requires native retained guards")
	}
	if err = guarded.ValidateExecutionGuards(ctx); err != nil {
		return err
	}
	canonical := pending.occurrence.ticket.frame
	effective := *canonical
	effective.Entity = pending.candidate.Index(pending.ordinal)
	effective.Previous = pending.current.ticket.previous
	effective.Action = h.WriteUpdate
	options, e := p.finiteCurrentValidationOptions(canonical, &effective)
	if e != nil {
		return e
	}
	validator, e := lookup[h.Validator](ctx, p.guardBinder, h.FrameworkValidatorKey)
	if e != nil {
		return e
	}
	finish, err = engine.BeginReconciliation(ctx)
	if err != nil {
		return err
	}
	values := reflect.MakeSlice(reflect.SliceOf(effective.Entity.Type()), 1, 1)
	values.Index(0).Set(effective.Entity)
	result, validationErr := validator.Validate(ctx, values.Interface(), []h.ValidationOptions{options})
	err = finiteCurrentValidationFailure(effective.Record.Path, result, validationErr)
	err = errors.Join(err, p.validateFinitePhaseTransition(ctx, pending.transition), p.validateFiniteRetainedActions())
	return err
}

func finiteCurrentValidationFailure(path string, result *h.Validation, cause error) error {
	if cause != nil {
		return fmt.Errorf("validate writer %s: %w", path, cause)
	}
	if violation := result.Err(); violation != nil {
		return rh.ValidationPhaseFailure(fmt.Errorf("validate writer %s: %w", path, violation))
	}
	return nil
}
