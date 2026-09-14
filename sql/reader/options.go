package reader

import (
	"context"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/sql/reader/readmeta"
	"github.com/viant/datly/sql/reader/rowcodec"
	"github.com/viant/sqlx"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	xcodec "github.com/viant/xdatly/codec"
	"reflect"
)

type readerOptions struct {
	retry             sqlxread.RetryPolicy
	readCache         cache.Cache
	refreshCache      bool
	cacheOnly         bool
	queryScope        *sqlxread.QueryScope
	codec             *rowcodec.Plan
	parameters        sqlx.ParameterResolver
	rowType           reflect.Type
	collectProjection bool
}

func newReaderOptions(session *Session, view *data.View) readerOptions {
	if session == nil || view == nil {
		return readerOptions{}
	}
	result := readerOptions{readCache: session.ReadCaches[view], parameters: session.Parameters}
	result.collectProjection = session.CollectProjection
	result.refreshCache = session.RefreshCache
	result.cacheOnly = session.CacheOnly
	result.queryScope = session.QueryScope
	if session.Artifact != nil {
		if plan := session.Artifact.ViewPlanFor(view); plan != nil {
			result.retry = (readRetry{source: session.SQL, connector: plan.Connector}).policy()
			result.codec = plan.Codec
			if plan.Collector != nil {
				result.rowType = plan.Collector.Schema.RowType()
			}
		}
	}
	return result
}

type rowRead struct {
	stats    *cache.Stats
	newRow   func() any
	options  []sqlxread.Option
	decoder  *rowcodec.Decoder
	evidence *columnEvidence
}

func (o readerOptions) rows(newRow func() any, resolve sqlxio.Resolve, matchers ...*cache.ParmetrizedQuery) rowRead {
	result := rowRead{newRow: newRow, options: o.withResolver(resolve, matchers...)}
	if o.readCache != nil {
		result.stats = &cache.Stats{}
		result.options = append(result.options, sqlxread.WithCacheStats(result.stats))
	}
	result.decoder = o.codec.NewDecoder(newRow, xcodec.WithValueLookup(func(_ context.Context, name string) (any, error) {
		if o.parameters == nil {
			return nil, fmt.Errorf("codec input parameter %s is unavailable", name)
		}
		value, found, err := o.parameters(name)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("codec input parameter %s is unavailable", name)
		}
		return value, nil
	}))
	if result.decoder != nil {
		result.newRow = result.decoder.NewRow
	}
	if o.collectProjection {
		result.evidence = &columnEvidence{rowType: o.rowType}
	}
	if result.decoder != nil || result.evidence != nil {
		result.options = append(result.options, sqlxread.WithColumnsObserver(func(columns []sqlxio.Column) error {
			if result.decoder != nil {
				if err := result.decoder.Observe(columns); err != nil {
					return err
				}
				if result.evidence != nil {
					indexes, err := result.decoder.FieldIndexes()
					if err != nil {
						return err
					}
					result.evidence.fields = readmeta.NewFields(o.rowType, indexes)
				}
				return nil
			}
			return result.evidence.observe(columns)
		}))
	}
	return result
}

func (o readerOptions) withResolver(resolve sqlxio.Resolve, matchers ...*cache.ParmetrizedQuery) []sqlxread.Option {
	options := []sqlxread.Option{sqlxread.WithUnmappedFn(resolve), sqlxread.WithRetry(o.retry), sqlxread.WithCacheOnly(o.cacheOnly), sqlxread.WithQueryScope(o.queryScope)}
	if o.readCache != nil {
		options = append(options, sqlxread.WithCache(o.readCache), sqlxread.WithCacheRefresh(cache.Refresh(o.refreshCache)))
	}
	if len(matchers) > 0 && matchers[0] != nil {
		options = append(options, sqlxread.WithInMatcher(matchers[0]))
	}
	return options
}
