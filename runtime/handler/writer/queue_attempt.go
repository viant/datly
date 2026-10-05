package writer

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"reflect"
)

func (p *Program) queueObserver() xhandler.QueueAttemptObserver {
	if !p.hook.IsValid() {
		return nil
	}
	observer, _ := p.hook.Interface().(xhandler.QueueAttemptObserver)
	return observer
}

func (p *Program) queue(ctx context.Context, binder xhandler.Binder) error {
	observer := p.queueObserver()
	var dml xhandler.DML
	var err error
	// Keep the default capability-resolution boundary unchanged.
	if observer == nil {
		dml, err = lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
		if err != nil {
			return err
		}
		for _, action := range p.actions.Rows {
			if err = p.queuePhysical(ctx, binder, dml, action, p.frameFor(action.Entity), nil); err != nil {
				return err
			}
		}
		return nil
	}
	physical := make(map[*Action]bool, len(p.actions.Rows))
	for _, action := range p.actions.Rows {
		physical[action] = true
	}
	for position, action := range p.queueItems {
		frame := p.frameFor(action.Entity)
		disposition := xhandler.QueuePhysical
		if !physical[action] {
			disposition = xhandler.QueueNoop
		}
		err = p.observeQueueItem(ctx, observer, position, action, frame, disposition, func(queued *bool) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if disposition == xhandler.QueueNoop {
				return nil
			}
			if dml == nil {
				var err error
				dml, err = lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
				if err != nil {
					return err
				}
			}
			return p.queuePhysical(ctx, binder, dml, action, frame, queued)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Program) observeQueueItem(ctx context.Context, observer xhandler.QueueAttemptObserver, position int, action *Action, frame *Frame, disposition xhandler.QueueDisposition, fn func(*bool) error) (err error) {
	queued := false
	p.notifyQueue(ctx, observer, position, action, frame, disposition, xhandler.PhaseBegin, "", nil, false)
	defer func() {
		if value := recover(); value != nil {
			p.notifyQueue(context.WithoutCancel(ctx), observer, position, action, frame, disposition, xhandler.PhaseEnd, xhandler.QueuePanicked, dexec.NewPanicError("writer queue item", value), queued)
			panic(value)
		}
		result := xhandler.QueueQueued
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			result = xhandler.QueueCanceled
		case err != nil:
			result = xhandler.QueueFailed
		case disposition == xhandler.QueueNoop:
			result = xhandler.QueueNoopCompleted
		}
		p.notifyQueue(context.WithoutCancel(ctx), observer, position, action, frame, disposition, xhandler.PhaseEnd, result, err, queued)
	}()
	return fn(&queued)
}

type detachedOriginal struct {
	fieldSet
	available bool
}

func (o detachedOriginal) Available() bool { return o.available }

func detachPresence(record *Record, source xhandler.FieldSet) fieldSet {
	result := fieldSet{}
	if source != nil {
		for _, field := range record.Fields {
			result[field.Name] = source.Has(field.Name)
		}
	}
	return result
}

func detachedRow(value reflect.Value) (result any, err error) {
	defer func() {
		if recover() != nil {
			result, err = nil, errors.New("queue row evidence clone panicked")
		}
	}()
	if !value.IsValid() {
		return nil, nil
	}
	return (xshape.Runtime{}).CloneValue(value.Interface())
}

func (p *Program) notifyQueue(ctx context.Context, observer xhandler.QueueAttemptObserver, position int, action *Action, frame *Frame, disposition xhandler.QueueDisposition, boundary xhandler.PhaseBoundary, result xhandler.QueueResult, cause error, queued bool) {
	// Include cloning in panic containment. Evidence failure cannot control work.
	defer func() {
		if recover() != nil && !p.queueObserverPanic {
			p.queueObserverPanic = true
			func() {
				defer func() { _ = recover() }()
				if logger := xlogger.FromContext(ctx); logger != nil {
					logger.Error("queue attempt observer panicked")
				}
			}()
		}
	}()
	id, attempt := p.queueInvocationID, 0
	if scope := rhandler.PhaseScopeFromContext(ctx); scope != nil {
		id, attempt = scope.InvocationID(), scope.Attempt()
	}
	if id == 0 {
		_, identity := rhandler.WithPhaseObserver(ctx, &queuePhaseObserver{}, 0, 0)
		p.queueInvocationID = identity.InvocationID()
		id = p.queueInvocationID
	}
	row, rowErr := detachedRow(frame.Entity)
	previous, previousErr := detachedRow(frame.Previous)
	event := xhandler.QueueAttemptEvent{InvocationID: id, Attempt: attempt, Position: position, Location: frame.Location, Role: frame.Record.Path, Operation: action.Kind, Disposition: disposition, Boundary: boundary, Result: result, Queued: queued, Cause: detachedQueueCause(cause), Row: row, Previous: previous, EvidenceError: errors.Join(rowErr, previousErr), Presence: detachPresence(frame.Record, frame.Fields), Original: detachedOriginal{detachPresence(frame.Record, frame.Original), frame.Original.Available()}}
	if frame.Previous.IsValid() {
		event.PreviousFields = p.previousFields[frame.Record].copy()
	}
	observer.ObserveQueueAttempt(ctx, event)
}

func (s fieldSet) copy() fieldSet {
	result := fieldSet{}
	for name, supplied := range s {
		result[name] = supplied
	}
	return result
}

// Supplies the engine's invocation/retry identity for a queue-only observer.
// The same invocation-local hook is bound later by the writer.
type queuePhaseObserver struct{ hook reflect.Value }

func (*queuePhaseObserver) ObservePhase(context.Context, xhandler.PhaseEvent) {}

// Retain identity for errors.Is without giving observers a mutable operational
// error (such as Conflict) that could change recovery or completion decisions.
type queueCause struct {
	message  string
	original error
}

func (e *queueCause) Error() string        { return e.message }
func (e *queueCause) Is(target error) bool { return errors.Is(e.original, target) }
func detachedQueueCause(cause error) error {
	if cause == nil {
		return nil
	}
	message := queueCauseMessage(cause)
	return &queueCause{message: message, original: cause}
}

func queueCauseMessage(cause error) (message string) {
	message = "queue operation failed (cause formatting panicked)"
	defer func() { _ = recover() }()
	return cause.Error()
}
