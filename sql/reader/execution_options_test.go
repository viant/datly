package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

type exactQueryProbe struct {
	cache.Cache
	options []interface{}
	sql     string
}

func (c *exactQueryProbe) Get(_ context.Context, sql string, _ []interface{}, options ...interface{}) (*cache.Entry, error) {
	c.sql, c.options = sql, options
	return nil, nil
}

type exactQueryLookupProbe struct{ *exactQueryProbe }

func (c *exactQueryLookupProbe) Lookup(_ context.Context, sql string, _ []interface{}, options ...interface{}) (*cache.Entry, error) {
	c.sql, c.options = sql, options
	return nil, cache.ErrMiss
}

func TestHintedCacheNeverReceivesWarmupMatchers(t *testing.T) {
	probe := &exactQueryProbe{}
	wrapped := onlyExactQueries(probe)
	_, lookup := wrapped.(cache.Lookup)
	require.False(t, lookup)
	matcher := &cache.ParmetrizedQuery{SQL: "logical", IdentitySQL: "old warmup", By: "id", In: []interface{}{1}}
	stats := &cache.Stats{}
	options := []interface{}{matcher, stats, cache.Refresh(false)}
	_, err := wrapped.Get(context.Background(), "SELECT /*+ hint */ id FROM items", []interface{}{1}, options...)
	require.NoError(t, err)
	require.Equal(t, "SELECT /*+ hint */ id FROM items", probe.sql)
	require.Equal(t, []interface{}{stats, cache.Refresh(false)}, probe.options)
	require.Same(t, matcher, options[0])
	require.Equal(t, "old warmup", matcher.IdentitySQL)
	lookupCache, ok := onlyExactQueries(&exactQueryLookupProbe{probe}).(cache.Lookup)
	require.True(t, ok)
	_, err = lookupCache.Lookup(context.Background(), "hinted SQL", nil, options...)
	require.ErrorIs(t, err, cache.ErrMiss)
	require.Equal(t, []interface{}{stats, cache.Refresh(false)}, probe.options)
}

func TestHintedRootAndRelationSkipWarmupIdentityPreparation(t *testing.T) {
	flag := false
	view := &data.View{Spec: spec.View{Name: "items", ExecutionOptions: &spec.ExecutionOptions{MySQL: &spec.MySQLExecutionOptions{ConditionFanoutFilter: &flag}}}, Cache: &data.Cache{Warmup: &spec.CacheWarmupSettings{IndexColumn: "id"}}}
	connection := dsql.Connection{Dialect: &info.Dialect{Product: database.Product{Name: "MySQL"}}}
	session := &Session{Artifact: &Plan{Root: &ViewPlan{View: view}}}
	query := &cache.ParmetrizedQuery{SQL: "SELECT id FROM items"}
	// Deliberately absent binder/snapshot/input: no identity preparation is allowed.
	require.NoError(t, (rootCacheMatcher{session: session, query: query}).apply(context.Background(), connection))
	relation := &relationRead{session: session, plan: &ViewPlan{View: view}, connection: connection}
	matcher := &cache.ParmetrizedQuery{SQL: query.SQL}
	require.NoError(t, relation.applyWarmupMatcher(context.Background(), query, matcher, []string{"id"}))
	require.Empty(t, matcher.IdentitySQL)
}
