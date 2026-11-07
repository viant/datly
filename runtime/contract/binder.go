package contract

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly"
)

// StatePlans belongs to one component/configuration generation. Compilation is
// provided by bootstrap's native parameter/codec owner; values and providers are
// supplied by each invocation. This does not replace scoped handler admission.
type StatePlans struct {
	compile func(reflect.Type) (*bindly.Plan, error)
	plans   sync.Map
}
type stateEntry struct {
	once sync.Once
	plan *bindly.Plan
	err  error
}

func NewStatePlans(compile func(reflect.Type) (*bindly.Plan, error)) *StatePlans {
	return &StatePlans{compile: compile}
}

// Binder adapts a native provider scope to compiled contract-state binding.
// It must be created only by the metadata/invocation scope owner.
func (p *StatePlans) Binder(scope bindly.Binder) bindly.Binder {
	return &stateBinder{plans: p, scope: scope}
}

type stateBinder struct {
	plans *StatePlans
	scope bindly.Binder
}

func (b *stateBinder) Bind(ctx context.Context, target any, options ...bindly.BindOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value := reflect.ValueOf(target)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("contract state must be a nonnil pointer to a struct")
	}
	if b.scope == nil || b.plans == nil || b.plans.compile == nil {
		return fmt.Errorf("contract state binding is unavailable")
	}
	typ := value.Elem().Type()
	actual, _ := b.plans.plans.LoadOrStore(typ, &stateEntry{})
	entry := actual.(*stateEntry)
	entry.once.Do(func() { entry.plan, entry.err = b.plans.compile(typ) })
	if entry.err != nil {
		return entry.err
	}
	// The native compiled plan is authoritative; no caller option can replace
	// Datly codec compilation with the raw tag-only plan.
	opts := make([]bindly.BindOption, 0, len(options)+1)
	opts = append(opts, options...)
	opts = append(opts, bindly.WithPlan(entry.plan))
	return b.scope.Bind(ctx, target, opts...)
}
