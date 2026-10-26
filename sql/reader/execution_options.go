package reader

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/executionhint"
	"github.com/viant/sqlx/io/read/cache"
)

func executionProduct(session *Session, view *data.View) (string, error) {
	if view == nil || view.Spec.ExecutionOptions == nil {
		return "", nil
	}
	if err := view.Spec.ExecutionOptions.Validate(); err != nil {
		return "", err
	}
	connector := view.Connector
	if session.Artifact != nil {
		if plan := session.Artifact.ViewPlanFor(view); plan != nil {
			connector = plan.Connector
		}
	}
	return session.SQL.IdentifierProduct(connector)
}

func rejectHintedWarmup(view *data.View, product string) error {
	if view != nil && view.Spec.ExecutionOptions.Active(product) {
		return fmt.Errorf("view %q: warmup is unsupported with active MySQL execution options; use exact-query lazy caching", view.Spec.Name)
	}
	return nil
}

func validateExecutionOptions(plan *Plan, sql *dsql.SQLComponent) error {
	if plan == nil {
		return nil
	}
	session := &Session{Artifact: plan, SQL: sql}
	seen := map[*ViewPlan]bool{}
	var visit func(*ViewPlan) error
	visit = func(p *ViewPlan) error {
		if p == nil || p.View == nil || seen[p] {
			return nil
		}
		seen[p] = true
		view := p.View
		product, err := executionProduct(session, view)
		if err != nil {
			return fmt.Errorf("view %q execution options: %w", view.Spec.Name, err)
		}
		if source := view.Spec.Source; source != nil && source.SQL != "" && !strings.ContainsAny(source.SQL, "$#") {
			if _, err := executionhint.Render(source.SQL, product, view.Spec.ExecutionOptions); err != nil {
				return fmt.Errorf("view %q: %w", view.Spec.Name, err)
			}
		}
		warmup := view.Cache != nil && view.Cache.HasWarmup()
		if source := view.Spec.Source; source != nil && source.Bindings != nil && source.Bindings.CacheWarmup != "" {
			warmup = true
		}
		hasOptions := view.Spec.ExecutionOptions != nil
		for _, relation := range p.Relations {
			if relation != nil && relation.Relation != nil && relation.Relation.IsOutput() && relation.Target != nil && relation.Target.View != nil && relation.Target.View.Spec.ExecutionOptions != nil {
				hasOptions = true
			}
		}
		if warmup && hasOptions {
			if view.Cache.HasWarmup() {
				policies, err := view.Cache.EffectiveWarmups()
				if err != nil {
					return err
				}
				for _, policy := range policies {
					work, err := warmupWorkItems(p, policy)
					if err != nil {
						return err
					}
					for _, item := range work {
						if item.plan.View.Spec.ExecutionOptions == nil {
							continue
						}
						product, err := sql.IdentifierProduct(item.connector())
						if err != nil {
							return err
						}
						if err := rejectHintedWarmup(item.plan.View, product); err != nil {
							return err
						}
					}
				}
			} else if err := rejectHintedWarmup(view, product); err != nil {
				return err
			}
		}
		for _, relation := range p.Relations {
			if relation != nil {
				if err := visit(relation.Target); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(plan.Root)
}

// The SQLX matcher also controls relation-local windows while reading rows.
// Keep it on the reader, but never pass it to a hinted view's native cache:
// native caches may derive and consult legacy warmup entries from it.
type exactQueryCache struct{ cache.Cache }

func exactCacheOptions(options []interface{}) []interface{} {
	result := make([]interface{}, 0, len(options))
	for _, option := range options {
		if _, matcher := option.(*cache.ParmetrizedQuery); !matcher {
			result = append(result, option)
		}
	}
	return result
}

func (c *exactQueryCache) Get(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	return c.Cache.Get(ctx, sql, args, exactCacheOptions(options)...)
}

type exactQueryLookupCache struct {
	*exactQueryCache
	lookup cache.Lookup
}

func (c *exactQueryLookupCache) Lookup(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	return c.lookup.Lookup(ctx, sql, args, exactCacheOptions(options)...)
}

func onlyExactQueries(service cache.Cache) cache.Cache {
	if service == nil {
		return nil
	}
	wrapped := &exactQueryCache{Cache: service}
	if lookup, ok := service.(cache.Lookup); ok {
		return &exactQueryLookupCache{exactQueryCache: wrapped, lookup: lookup}
	}
	return wrapped
}
