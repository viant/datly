package reader

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/cache/managed"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	aerospikecache "github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type testAuthorizedIndexPredicate struct{}

func (*testAuthorizedIndexPredicate) Compute(ctx context.Context, value interface{}) (*codec.Criteria, error) {
	if view.IsCacheIndexIdentity(ctx) {
		return nil, nil
	}
	if value != 77 {
		view.DenyCacheIndex(ctx)
		return &codec.Criteria{Expression: "1=0"}, nil
	}
	view.SelectCacheIndex(ctx, "order_id", []interface{}{value})
	return &codec.Criteria{Expression: "m.order_id = ?", Placeholders: []interface{}{value}}, nil
}

func TestAuthorizedCacheIndexLocalAerospike(t *testing.T) {
	if os.Getenv("DATLY_TEST_AEROSPIKE") == "" {
		t.Skip("set DATLY_TEST_AEROSPIKE for the local indexed cache test")
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "performance.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE items(order_id INTEGER, total_spend REAL); INSERT INTO items VALUES (77, 12.5), (88, 40.0)")
	require.NoError(t, err)

	orderParam := state.NewParameter("OrderId", state.NewQueryLocation("order_id"), state.WithParameterType(reflect.TypeOf([]int{})))
	authParam := state.NewParameter("AuthID", state.NewQueryLocation("auth_id"), state.WithParameterType(reflect.TypeOf(0)))
	authParam.Predicates = []*extension.PredicateConfig{{Name: "handler", Group: 0, Ensure: true, Args: []string{"*predicate.TestAuthorizedIndex"}}}
	aView := view.NewView("performance", "performance",
		view.WithConnector(view.NewConnector("db", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "order_id", DataType: "int", Groupable: true},
			{Name: "total_spend", DataType: "float", Aggregate: true},
		}),
		view.WithTemplate(view.NewTemplate("SELECT m.order_id,m.total_spend FROM items m WHERE 1=1 ${predicate.Builder().CombineOr($predicate.FilterGroup(0, \"AND\")).Build(\"AND\")}", view.WithTemplateParameters(orderParam, authParam))),
	)
	resource := view.EmptyResource()
	require.NoError(t, resource.Init(ctx, extension.Config))
	require.NoError(t, resource.TypeRegistry().Register("TestAuthorizedIndex", xreflect.WithPackage("predicate"), xreflect.WithReflectType(reflect.TypeOf(testAuthorizedIndexPredicate{}))))
	require.NoError(t, aView.Init(ctx, resource))
	aView.Cache = &view.Cache{Warmup: &view.Warmup{IndexColumn: "order_id", IndexParameter: "order_id"}}
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, statelet.Template.SetValue("AuthID", 77))
	requestCtx := view.WithCacheIndexSelection(ctx)
	var output interface{}
	readSession, err := NewSession(&output, aView)
	require.NoError(t, err)
	require.NoError(t, readSession.Init())
	collector := aView.Collector(&output, nil, false)
	mainSQL, matcher, err := New().buildParametrizedSQL(requestCtx, aView, statelet, nil, collector, readSession, nil)
	require.NoError(t, err)
	require.NotNil(t, matcher)
	require.NotNil(t, mainSQL)
	require.Contains(t, mainSQL.SQL, "m.order_id = ?")
	require.NotContains(t, matcher.SQL, "m.order_id = ?")
	require.Equal(t, "order_id", matcher.By)
	require.Equal(t, []interface{}{77}, matcher.In)

	client, err := as.NewClient("127.0.0.1", 3000)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	set := "datly_index_" + uuid.NewString()[:8]
	native, err := aerospikecache.New("ns_memory", set, client, 30)
	require.NoError(t, err)
	store, err := managed.NewAerospikeStore(client, "ns_memory", set, "performance")
	require.NoError(t, err)
	cacheService := managed.New(native, store, "performance", nil)
	t.Cleanup(func() { _, _ = cacheService.Invalidate(ctx, managed.All) })
	_, err = cacheService.Invalidate(ctx, managed.All)
	require.NoError(t, err)
	beforeReset, err := store.Read(ctx)
	require.NoError(t, err)
	_, err = cacheService.IndexBy(ctx, db, "order_id", matcher.SQL, matcher.Args)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE items SET total_spend = 99 WHERE order_id = 77")
	require.NoError(t, err)
	var directSpend float64
	require.NoError(t, db.QueryRow("SELECT total_spend FROM items WHERE order_id = 77").Scan(&directSpend))
	require.Equal(t, 99.0, directSpend)

	type row struct {
		OrderID int     `sqlx:"order_id"`
		Spend   float64 `sqlx:"total_spend"`
	}
	stats := &cache.Stats{}
	reader, err := read.New(ctx, db, mainSQL.SQL, func() interface{} { return &row{} },
		read.WithCache(cacheService), read.WithInMatcher(matcher), read.WithCacheStats(stats))
	require.NoError(t, err)
	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	var got []*row
	err = reader.QueryAll(ctx, func(value interface{}) error { got = append(got, value.(*row)); return nil }, mainSQL.Args...)
	require.NoError(t, err)
	require.True(t, stats.FoundWarmup)
	require.Len(t, got, 1)
	require.Equal(t, 12.5, got[0].Spend)
	require.NoError(t, statelet.Template.SetValue("AuthID", 88))
	deniedSQL, deniedMatcher, err := New().buildParametrizedSQL(view.WithCacheIndexSelection(ctx), aView, statelet, nil, collector, readSession, nil)
	require.NoError(t, err)
	require.Nil(t, deniedMatcher)
	require.Contains(t, deniedSQL.SQL, "1=0")
	deniedReader, err := read.New(ctx, db, deniedSQL.SQL, func() interface{} { return &row{} }, read.WithCache(cacheService))
	require.NoError(t, err)
	defer func() {
		if stmt := deniedReader.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	got = nil
	err = deniedReader.QueryAll(ctx, func(value interface{}) error { got = append(got, value.(*row)); return nil }, deniedSQL.Args...)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, statelet.Template.SetValue("AuthID", 77))

	_, err = cacheService.Invalidate(ctx, managed.All)
	require.NoError(t, err)
	afterReset, err := store.Read(ctx)
	require.NoError(t, err)
	require.NotEqual(t, beforeReset.All, afterReset.All)
	_, matcherAfterReset, err := New().buildParametrizedSQL(view.WithCacheIndexSelection(ctx), aView, statelet, nil, collector, readSession, nil)
	require.NoError(t, err)
	afterNative, err := aerospikecache.New("ns_memory", set, client, 30)
	require.NoError(t, err)
	afterCacheService := managed.New(afterNative, store, "performance", nil)
	stats = &cache.Stats{}
	readerAfterReset, err := read.New(ctx, db, mainSQL.SQL, func() interface{} { return &row{} },
		read.WithCache(afterCacheService), read.WithInMatcher(matcherAfterReset), read.WithCacheStats(stats))
	require.NoError(t, err)
	defer func() {
		if stmt := readerAfterReset.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	got = nil
	err = readerAfterReset.QueryAll(ctx, func(value interface{}) error { got = append(got, value.(*row)); return nil }, mainSQL.Args...)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 99.0, got[0].Spend)
	require.False(t, stats.FoundWarmup)
}
