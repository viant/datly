package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/responsebinding"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xbind "github.com/viant/xdatly/bind"
	xhandler "github.com/viant/xdatly/handler"
)

var ErrBeforeCommitReadOnly = errors.New("before-commit validation admits only native reads on existing transactions")

type beforeCommitReadKey struct{}
type beforeCommitFrame struct {
	ctx      context.Context
	binder   xhandler.Binder
	validate xhandler.BeforeCommitValidator
	owner    *dataScope
	mu       sync.Mutex
	active   bool
	failure  error
	next     uint64
	calls    map[uint64]context.CancelCauseFunc
}

func beforeCommitRead(ctx context.Context) *beforeCommitFrame {
	p, _ := ctx.Value(beforeCommitReadKey{}).(*beforeCommitFrame)
	return p
}
func (p *beforeCommitFrame) reject() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failure = errors.Join(p.failure, ErrBeforeCommitReadOnly)
	return ErrBeforeCommitReadOnly
}
func (p *beforeCommitFrame) admit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		p.failure = errors.Join(p.failure, ErrBeforeCommitReadOnly)
	}
	return p.failure
}

// The callback retains its caller context, but no pending database ownership.
// Only Invoke installs the private transaction reader for its bounded call.
type beforeCommitContext struct{ context.Context }

func (c beforeCommitContext) Value(key any) any {
	switch key.(type) {
	case dataScopeContextKey, componentScopeKey, readerInvocationKey, readOnlyDependencyKey, responsebinding.Key, responsebinding.ChildKey, beforeCommitReadKey:
		return nil
	}
	return c.Context.Value(key)
}
func (p *beforeCommitFrame) Invoke(call context.Context, request xhandler.ComponentRequest) (any, error) {
	if call == nil || request.Rows != nil || request.Completion != nil {
		return nil, p.reject()
	}
	p.mu.Lock()
	if !p.active || p.failure != nil {
		p.mu.Unlock()
		return nil, p.reject()
	}
	p.next++
	id := p.next
	ctx, cancel := context.WithCancelCause(lifecycleCallContext{Context: p.ctx, call: call})
	stop := context.AfterFunc(call, func() { cancel(context.Cause(call)) })
	p.calls[id] = cancel
	p.mu.Unlock()
	defer func() { stop(); cancel(nil); p.mu.Lock(); delete(p.calls, id); p.mu.Unlock() }()
	ctx = context.WithValue(beforeCommitContext{ctx}, beforeCommitReadKey{}, p)
	ctx = dexec.WithInvocationTransactionLookup(ctx, p.transaction)
	invoker, err := xbind.MustLookup[xhandler.ComponentInvoker](ctx, p.binder, xhandler.ComponentInvokerKey)
	if err != nil {
		return nil, err
	}
	result, err := invoker.Invoke(ctx, request)
	if criticalReadOnlyFailure(err) {
		p.reject()
	}
	if closeErr := p.admit(); closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	return result, err
}
func (p *beforeCommitFrame) transaction(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if err := p.admit(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.owner.mu.Lock()
	unit := p.owner.bySource[db]
	p.owner.mu.Unlock()
	if unit == nil || !unit.ownerResolved || unit.data == nil {
		return nil, p.reject()
	}
	owned, ok := unit.data.(interface{ InvocationTransaction() (*sql.DB, *sql.Tx) })
	if !ok {
		return nil, p.reject()
	}
	actual, tx := owned.InvocationTransaction()
	if actual != db || tx == nil {
		return nil, p.reject()
	}
	return tx, nil
}
func (s *dataScope) registerBeforeCommit(ctx context.Context, binder xhandler.Binder, validate xhandler.BeforeCommitValidator) error {
	root := rootScope(s)
	root.mu.Lock()
	defer root.mu.Unlock()
	if root.completionStarted {
		return fmt.Errorf("before-commit registration after completion started")
	}
	root.beforeCommit = append(root.beforeCommit, &beforeCommitFrame{ctx: dexec.WithInvocationTransactionLookup(beforeCommitContext{ctx}, nil), binder: binder, validate: validate, owner: root})
	return nil
}
func (s *dataScope) validateBeforeCommit(ctx context.Context, units []*dataScope) (err error) {
	root := rootScope(s)
	root.mu.Lock()
	frames := append([]*beforeCommitFrame(nil), root.beforeCommit...)
	root.mu.Unlock()
	if len(frames) == 0 {
		return nil
	}
	// Validation must observe drained writes while commit ownership remains
	// with the runtime. Flush-only owners cannot provide that boundary.
	for _, unit := range units {
		if unit.data == nil {
			continue
		}
		if _, ok := unit.data.(invocationDataPreparer); !ok {
			return fmt.Errorf("before-commit validation requires a data owner with completion preparation")
		}
	}
	for _, frame := range frames {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = frame.run(); err != nil {
			return err
		}
	}
	return nil
}
func (p *beforeCommitFrame) run() (err error) {
	p.mu.Lock()
	p.active = true
	p.calls = map[uint64]context.CancelCauseFunc{}
	p.mu.Unlock()
	defer func() {
		if v := recover(); v != nil {
			err = dexec.NewPanicError("before-commit validation", v)
		}
		p.mu.Lock()
		p.active = false
		if len(p.calls) > 0 {
			p.failure = errors.Join(p.failure, ErrBeforeCommitReadOnly)
		}
		for _, cancel := range p.calls {
			cancel(ErrBeforeCommitReadOnly)
		}
		err = errors.Join(err, p.failure, p.ctx.Err())
		p.mu.Unlock()
	}()
	return p.validate.BeforeCommit(p.ctx, p)
}
func beforeCommitProviders(ctx context.Context, providers []locator.Provider) []locator.Provider {
	p := beforeCommitRead(ctx)
	if p == nil {
		return providers
	}
	kinds := []string{string(xhandler.DataKey), string(xhandler.DMLKey), string(xhandler.SequencerKey), string(xhandler.FlusherKey), string(xhandler.TransactionStarterKey), string(xhandler.MessageBusKey), string(xhandler.ConnectorKey), string(rhandler.TransactionSQLCapabilityKey)}
	blockedKinds := map[string]bool{}
	for _, kind := range kinds {
		blockedKinds[kind] = true
	}
	out := make([]locator.Provider, 0, len(providers)+len(kinds))
	for _, provider := range providers {
		if !blockedKinds[provider.Kind()] {
			out = append(out, provider)
		}
	}
	for _, kind := range kinds {
		out = append(out, handlerprovider.Named(kind, func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, p.reject() }))
	}
	return out
}
