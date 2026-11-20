// Package association connects the effective tool's normalized edits to the
// native component invocation. No part of this registry is decoded from JSON.
package association

import (
	"context"
	"reflect"
	"sync/atomic"

	"github.com/viant/datly/exec"
	"github.com/viant/xdatly/handler"
)

type Delta struct{ Added, Deleted map[any]struct{} }

func contains(ids map[any]struct{}, member any) bool {
	if member == nil || !reflect.TypeOf(member).Comparable() {
		return false
	}
	_, ok := ids[member]
	return ok
}

type readOnlyDelta struct{ value Delta }

func (d readOnlyDelta) AddedCount() int       { return len(d.value.Added) }
func (d readOnlyDelta) DeletedCount() int     { return len(d.value.Deleted) }
func (d readOnlyDelta) IsAdded(id any) bool   { return contains(d.value.Added, id) }
func (d readOnlyDelta) IsDeleted(id any) bool { return contains(d.value.Deleted, id) }

type key struct {
	holder      reflect.Type
	field, path string
}
type Edits map[key]Delta

func (e Edits) Set(holder reflect.Type, field, path string, delta Delta) {
	e[key{holder, field, path}] = delta
}

type registry struct {
	target handler.ComponentTarget
	edits  atomic.Pointer[Edits]
}

// New creates an invocation-local lookup before effective metadata is resolved.
// Publish installs a complete immutable snapshot only after argument preparation.
func New(ctx context.Context, target exec.ComponentTarget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, handler.AssociationDeltaLookupKey, &registry{target: portableTarget(target)})
}
func portableTarget(target exec.ComponentTarget) handler.ComponentTarget {
	return handler.ComponentTarget{Scope: target.Component.Scope, Name: target.Component.Name, Method: target.Route.Method, Path: target.Route.Path}
}

// TransactionContext selects the isolation needed for the authorized current
// membership read and write to share a single serializable view.
func TransactionContext(ctx context.Context, target exec.ComponentTarget) context.Context {
	lookup, ok := ctx.Value(handler.AssociationDeltaLookupKey).(*registry)
	if !ok || !lookup.ActiveAssociationDelta(portableTarget(target)) {
		return ctx
	}
	return exec.WithTransactionIsolation(ctx, exec.IsolationSerializable)
}

func Available(ctx context.Context) bool {
	_, ok := ctx.Value(handler.AssociationDeltaLookupKey).(*registry)
	return ok
}
func Publish(ctx context.Context, edits Edits) {
	if lookup, ok := ctx.Value(handler.AssociationDeltaLookupKey).(*registry); ok && len(edits) > 0 {
		lookup.edits.Store(&edits)
	}
}
func (r *registry) ActiveAssociationDelta(target handler.ComponentTarget) bool {
	return r.target == target && r.edits.Load() != nil
}

func (r *registry) LookupAssociationDelta(target handler.ComponentTarget, holder reflect.Type, field, path string) (handler.AssociationDelta, bool) {
	if target != r.target {
		return nil, false
	}
	snapshot := r.edits.Load()
	if snapshot == nil {
		return nil, false
	}
	for holder != nil && holder.Kind() == reflect.Pointer {
		holder = holder.Elem()
	}
	d, ok := (*snapshot)[key{holder, field, path}]
	if !ok {
		return nil, false
	}
	return readOnlyDelta{d}, true
}
