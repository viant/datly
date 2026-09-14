package bootstrap

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

// CompiledReader exposes the immutable reader products needed to attach a
// concrete reader execution without exposing the containing bootstrap artifact.
type CompiledReader struct {
	component  *spec.Component
	inputType  reflect.Type
	outputType reflect.Type
	plan       *sqlreader.Plan
}

// ReaderRuntimeConfig supplies concrete execution dependencies without
// exposing compiled reader metadata to composition callers.
type ReaderRuntimeConfig struct {
	// Aerospike is a caller-owned native client pool, shared across compiled readers.
	Aerospike                *aerospike.Pool
	SQL                      *dsql.SQLComponent
	ReadCaches               map[string]cache.Cache
	CacheSettings            map[string]*spec.CacheSettings
	RelationFetchConcurrency int
}

// ReaderCompilation returns the compiled reader products, if the component
// declares views.
func (a *Artifact) ReaderCompilation() *CompiledReader {
	if a == nil || a.Reader == nil {
		return nil
	}
	return &CompiledReader{
		component: a.Component, inputType: a.inputType, outputType: a.outputType, plan: a.Reader,
	}
}

// NewExecution attaches concrete runtime dependencies and returns only the
// unified reader capability.
func (c *CompiledReader) NewExecution(config ReaderRuntimeConfig) (exec.Reader, error) {
	if c == nil {
		return nil, fmt.Errorf("compiled reader is required")
	}
	readCaches, err := c.resolveCaches(config)
	if err != nil {
		return nil, err
	}
	var options []sqlreader.Option
	if config.RelationFetchConcurrency > 0 {
		options = append(options, sqlreader.WithRelationFetchConcurrency(config.RelationFetchConcurrency))
	}
	return sqlreader.NewExecution(sqlreader.Config{
		Component: c.component, InputType: c.inputType, OutputType: c.outputType,
		Plan: c.plan, SQL: config.SQL, ReadCaches: readCaches,
	}, options...)
}

func (c *CompiledReader) resolveCaches(config ReaderRuntimeConfig) (map[*data.View]cache.Cache, error) {
	if c.plan == nil || c.plan.ViewIndex == nil || c.component == nil {
		return nil, fmt.Errorf("compiled reader cache metadata is incomplete")
	}
	result := make(map[*data.View]cache.Cache, len(config.ReadCaches))
	for alias, service := range config.ReadCaches {
		view, err := c.plan.ViewIndex.Resolve(alias)
		if err != nil {
			return nil, fmt.Errorf("resolve reader cache %q: %w", alias, err)
		}
		if _, ok := result[view]; ok {
			return nil, fmt.Errorf("duplicate reader cache for view %q", alias)
		}
		if service == nil || reflect.ValueOf(service).Kind() == reflect.Pointer && reflect.ValueOf(service).IsNil() {
			return nil, fmt.Errorf("reader cache for view %q is nil", alias)
		}
		result[view] = service
	}
	visited := map[*data.View]bool{}
	var visit func(*sqlreader.ViewPlan, string, bool) error
	visit = func(plan *sqlreader.ViewPlan, path string, output bool) error {
		if plan == nil || plan.View == nil || visited[plan.View] {
			return nil
		}
		view := plan.View
		visited[view] = true
		var settings *spec.CacheSettings
		if view.Cache != nil {
			settings = config.CacheSettings[view.Cache.Name]
		}
		if settings == nil && plan == c.plan.Root && c.component.Settings != nil && c.component.Settings.Cache != nil {
			settings = c.component.Settings.Cache
		}
		if settings == nil && output && c.component.Settings != nil && c.component.Settings.Cache != nil {
			root := c.component.Settings.Cache
			if root.Warmup != nil && root.Warmup.IndexMeta {
				settings = root
			}
		}
		if result[view] == nil {
			if settings != nil && settings.Enabled {
				service, err := (cacheconfig.Config{Settings: settings, Aerospike: config.Aerospike, Identity: c.component.Key.String() + ":" + path + ":" + view.Connector}).New()
				if err != nil {
					return err
				}
				result[view] = service
			} else if view.Cache != nil && (view.Cache.Name != "" || view.Cache.Warmup != nil) {
				return fmt.Errorf("view %q cache %q has no enabled configuration or supplied service", view.Spec.Name, view.Cache.Name)
			}
		}
		for i, relation := range plan.Relations {
			if relation == nil {
				return fmt.Errorf("view %q has an incomplete relation", view.Spec.Name)
			}
			if err := visit(relation.Target, fmt.Sprintf("%s/%d", path, i), relation.Relation != nil && relation.Relation.IsOutput()); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(c.plan.Root, "root", false); err != nil {
		return nil, err
	}
	return result, nil
}
