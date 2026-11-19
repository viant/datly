package reader

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xreflect"
)

type authorizedFallbackRow struct {
	OrderID    int     `sqlx:"order_id"`
	CampaignID int     `sqlx:"campaign_id"`
	Spend      float64 `sqlx:"total_spend"`
}

func TestAuthorizedUnwarmedIndexExecutesScopedQuery(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "records.db")
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(ctx, `CREATE TABLE items(order_id INTEGER,campaign_id INTEGER,total_spend REAL);
INSERT INTO items VALUES(77,101,12.5),(88,101,40.0)`)
	require.NoError(t, err)
	campaign := state.NewParameter("CampaignIds", state.NewQueryLocation("campaign_id"), state.WithParameterType(reflect.TypeOf([]int{})))
	auth := state.NewParameter("AuthID", state.NewQueryLocation("auth_id"), state.WithParameterType(reflect.TypeOf(0)))
	auth.Predicates = []*extension.PredicateConfig{{Name: "handler", Group: 0, Ensure: true, Args: []string{"*predicate.TestAuthorizedIndex"}}}
	connector := view.NewConnector("db", "sqlite3", dsn)
	aView := view.NewView("performance", "performance",
		view.WithConnector(connector),
		view.WithViewType(reflect.TypeOf([]*authorizedFallbackRow{})),
		view.WithColumns(view.Columns{
			{Name: "order_id", DataType: "int"},
			{Name: "campaign_id", DataType: "int"},
			{Name: "total_spend", DataType: "float"},
		}),
		view.WithTemplate(view.NewTemplate(`SELECT m.order_id,m.campaign_id,m.total_spend FROM items m WHERE 1=1 ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}`, view.WithTemplateParameters(campaign, auth))),
	)
	aView.Cache = &view.Cache{Location: t.TempDir(), TimeToLiveMs: 60000, Warmup: &view.Warmup{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"}}
	resource := view.EmptyResource()
	require.NoError(t, resource.Init(ctx, extension.Config))
	require.NoError(t, resource.TypeRegistry().Register("TestAuthorizedIndex", xreflect.WithPackage("predicate"), xreflect.WithReflectType(reflect.TypeOf(testAuthorizedIndexPredicate{}))))
	require.NoError(t, aView.Init(ctx, resource))
	viewDB, err := aView.Db()
	require.NoError(t, err)
	t.Cleanup(func() { _ = viewDB.Close() })
	service := New()
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, campaign.Set(statelet.Template, []int{101}))
	require.NoError(t, auth.Set(statelet.Template, 77))
	// Populate a campaign group containing both authorized and unauthorized rows.
	// Index-identity generation has its existing predicate-specific semantics.
	warmed, err := service.warmupMatcher(view.WithCacheIndexIdentity(ctx), aView, aView.Cache.Warmup, statelet, nil)
	require.NoError(t, err)
	require.NotNil(t, warmed)
	ensureWarmupIdentity(warmed)
	warmed.By, warmed.In = "campaign_id", []interface{}{101}
	native, err := aView.Cache.Service()
	require.NoError(t, err)
	_, err = native.IndexBy(ctx, db, "campaign_id", warmed.SQL, warmed.Args, warmed)
	require.NoError(t, err)
	// Prove the disallowed index is populated, rather than merely testing an empty cache.
	stats := &cache.Stats{}
	probe, err := read.New(ctx, db, warmed.SQL, func() interface{} { return &authorizedFallbackRow{} }, read.WithCache(native), read.WithInMatcher(warmed), read.WithCacheStats(stats))
	require.NoError(t, err)
	var broad []*authorizedFallbackRow
	err = probe.QueryAll(ctx, func(value interface{}) error { broad = append(broad, value.(*authorizedFallbackRow)); return nil }, warmed.Args...)
	if stmt := probe.Stmt(); stmt != nil {
		require.NoError(t, stmt.Close())
	}
	require.NoError(t, err)
	require.True(t, stats.FoundWarmup)
	require.Len(t, broad, 2)
	_, err = db.ExecContext(ctx, "UPDATE items SET total_spend=99 WHERE order_id=77")
	require.NoError(t, err)

	// The real reader must miss that campaign warmup, execute the order-scoped
	// SQL and return the fresh authorized row, despite the campaign input.
	for _, tc := range []struct {
		authID int
		want   []*authorizedFallbackRow
	}{
		{77, []*authorizedFallbackRow{{OrderID: 77, CampaignID: 101, Spend: 99}}},
		{88, nil},
	} {
		selectors := view.NewState()
		selectors.Init(aView)
		request := selectors.Lookup(aView)
		require.NoError(t, auth.Set(request.Template, tc.authID))
		require.NoError(t, campaign.Set(request.Template, []int{101}))
		var rows []*authorizedFallbackRow
		require.NoError(t, service.ReadInto(ctx, &rows, aView, WithResourceState(selectors)))
		require.Equal(t, tc.want, rows)
	}
}
