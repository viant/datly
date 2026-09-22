package bootstrap

import (
	"fmt"
	"github.com/viant/datly/constant"
	"reflect"
	"strings"

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
	instanceConst *constant.Values
	component     *spec.Component
	inputType     reflect.Type
	outputType    reflect.Type
	plan          *sqlreader.Plan
}

// ReaderRuntimeConfig supplies concrete execution dependencies without
// exposing compiled reader metadata to composition callers.
type ReaderRuntimeConfig struct {
	// CacheIdentity identifies concrete connector configuration without credentials.
	CacheIdentity string
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
		instanceConst: a.instanceConst, component: a.Component, inputType: a.inputType, outputType: a.outputType, plan: a.Reader,
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
		warmupBinding := viewWarmupBinding(view)
		if warmupBinding != "" {
			if view.Cache == nil {
				view.Cache = &data.Cache{}
			}
			if !view.Cache.HasWarmup() {
				warmupSettings := config.CacheSettings[warmupBinding]
				if warmupSettings == nil || !warmupSettings.HasWarmup() {
					return fmt.Errorf("view %q cache warmup %q has no configuration", view.Spec.Name, warmupBinding)
				}
				view.Cache.Warmup = warmupSettings.Warmup.Clone()
				for _, item := range warmupSettings.Warmups {
					view.Cache.Warmups = append(view.Cache.Warmups, item.Clone())
				}
				if view.Cache.SharedCases == nil {
					view.Cache.SharedCases = spec.CloneSharedCases(warmupSettings.SharedCases)
				}
			}
		}
		var settings *spec.CacheSettings
		if view.Cache != nil {
			settings = config.CacheSettings[view.Cache.Name]
		}
		if settings == nil && warmupBinding != "" && view.Cache != nil && view.Cache.Name == "" {
			settings = config.CacheSettings[warmupBinding]
		}
		if settings == nil && plan == c.plan.Root && c.component.Settings != nil && c.component.Settings.Cache != nil {
			settings = c.component.Settings.Cache
		}
		if settings == nil && output && c.component.Settings != nil && c.component.Settings.Cache != nil {
			root := c.component.Settings.Cache
			if rootWarmups, err := root.EffectiveWarmups(); err != nil {
				return err
			} else {
				for _, item := range rootWarmups {
					if item.IndexMeta {
						settings = root
						break
					}
				}
			}
		}
		if result[view] == nil {
			if settings != nil && settings.Enabled {
				accessSettings := *settings
				settings = &accessSettings
				var err error
				settings.Location, err = c.instanceConst.Path(cacheconfig.ExpandLocation(settings.Location, &view.Spec))
				if err != nil {
					return err
				}
				service, err := (cacheconfig.Config{Settings: settings, Aerospike: config.Aerospike, Identity: c.component.Key.String() + ":" + path + ":" + view.Connector + ":" + config.CacheIdentity + ":" + c.instanceConst.Identity()}).New()
				if err != nil {
					return err
				}
				result[view] = service
			} else if view.Cache != nil && (view.Cache.Name != "" || view.Cache.HasWarmup()) {
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

func viewWarmupBinding(view *data.View) string {
	if view == nil || view.Spec.Source == nil || view.Spec.Source.Bindings == nil {
		return ""
	}
	return strings.TrimSpace(view.Spec.Source.Bindings.CacheWarmup)
}
