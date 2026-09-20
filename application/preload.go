package application

import (
	"context"
	"fmt"
	"reflect"
	"time"

	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xlogger "github.com/viant/xdatly/logger"
)

// Startup consumers such as warmup credential validation need the transitive
// input contracts, not just the root registration. Load only their dependency
// closure; do not execute binding or eagerly compile unrelated components.
func preloadComponents(ctx context.Context, lease *bootstrapindex.Lease, roots []spec.Key, logger xlogger.Logger) ([]*registry.RegisteredComponent, error) {
	var result []*registry.RegisteredComponent
	seen := map[spec.Key]bool{}
	routes := map[string]bool{}
	load := func(key spec.Key) error {
		if seen[key] {
			return nil
		}
		started := time.Now()
		loaded, err := lease.LoadComponent(ctx, key)
		if err != nil {
			logIndexedPreload(logger, key, 0, started, err)
			return err
		}
		logIndexedPreload(logger, key, len(loaded.Related), started, nil)
		family := append([]*registry.RegisteredComponent{loaded.Registration}, loaded.Related...)
		for _, reg := range family {
			if reg == nil || reg.Component == nil || reg.Input == nil {
				return fmt.Errorf("preloaded component %s requires a compiled input contract", key.String())
			}
			if seen[reg.Component.Key] {
				continue
			}
			seen[reg.Component.Key] = true
			result = append(result, reg)
			for _, endpoint := range reg.Component.Routes {
				if endpoint != nil {
					routes[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()] = true
				}
			}
		}
		return nil
	}
	for _, key := range roots {
		if err := load(key); err != nil {
			return nil, err
		}
	}
	// Newly loaded dependencies extend the queue. Shared dependencies and cycles
	// are visited once; the canonical InputCatalog still validates cycles/policies.
	for i := 0; i < len(result); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reg := result[i]
		for _, endpoint := range reg.Component.Routes {
			if endpoint == nil {
				return nil, fmt.Errorf("preloaded component %s has a nil route", reg.Component.Key.String())
			}
			ref := spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}
			contract, ok := reg.Input.ForRoute(ref)
			if !ok {
				return nil, fmt.Errorf("preloaded input contract not found: %s", ref.String())
			}
			for _, field := range contract.Fields() {
				target, dependency := field.Dependency()
				if !dependency || routes[target.String()] {
					continue
				}
				entry, route, _, found := lease.Snapshot().Route(target.Method, target.Path)
				if !found || route.Path != target.Path {
					return nil, fmt.Errorf("%s input %s: component dependency route contract not found: %s", ref.String(), field.Path(), target.String())
				}
				if err := load(entry.Key()); err != nil {
					return nil, fmt.Errorf("%s input %s: %w", ref.String(), field.Path(), err)
				}
			}
		}
	}
	return result, nil
}

func logIndexedPreload(logger xlogger.Logger, key spec.Key, related int, started time.Time, err error) {
	if nilBootstrapLogger(logger) {
		return
	}
	args := []any{"component", key.String(), "related", related, "elapsed", time.Since(started).String()}
	if err != nil {
		args = append(args, "status", "error", "error", err)
		logger.Error("datly bootstrap indexed preload", args...)
		return
	}
	args = append(args, "status", "ok")
	logger.Info("datly bootstrap indexed preload", args...)
}

func nilBootstrapLogger(logger xlogger.Logger) bool {
	if logger == nil {
		return true
	}
	value := reflect.ValueOf(logger)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
