package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	xexec "github.com/viant/xdatly/exec"
	xstate "github.com/viant/xdatly/state"
)

type warmupOrderedRow struct {
	AdOrderID       int    `sqlx:"ad_order_id"`
	AdvertiserDate  string `sqlx:"advertiser_date"`
	ChannelID       int    `sqlx:"channel_id"`
	Bids            int    `sqlx:"bids"`
	Clicks          int    `sqlx:"clicks"`
	Conversions     int    `sqlx:"conversions"`
	Impressions     int    `sqlx:"impressions"`
	TotalSpend      int    `sqlx:"total_spend"`
	V100            int    `sqlx:"v100"`
	VStart          int    `sqlx:"v_start"`
	ViewConversions int    `sqlx:"view_conversions"`
}

const warmupOrderedSQL = `SELECT ao.ad_order_id,ao.advertiser_date,ao.channel_id,
SUM(ao.bids) AS bids,SUM(ao.bids) AS clicks,SUM(ao.bids) AS conversions,
SUM(ao.bids) AS impressions,SUM(ao.bids) AS total_spend,SUM(ao.bids) AS v100,
SUM(ao.bids) AS v_start,SUM(ao.bids) AS view_conversions
FROM records ao GROUP BY ao.ad_order_id,ao.advertiser_date,ao.channel_id`

func warmupOrderedFields(channel bool) []string {
	fields := []string{"ad_order_id", "advertiser_date", "bids", "clicks", "conversions", "impressions", "total_spend", "v100", "v_start", "view_conversions"}
	if channel {
		fields = append(fields, "channel_id")
	}
	return fields
}

func TestRootWarmupOrderingFallsBackSQLite(t *testing.T) {
	for _, compatible := range []bool{false, true} {
		for _, tc := range []struct {
			name, order   string
			limit, offset int
			want          []int
		}{
			{"canonical without request sort", "", 0, 0, []int{1, 2, 3, 4}},
			{"payload ordering", "advertiser_date asc,channel_id asc", 50, 0, []int{1, 2, 3, 4}},
			{"ascending window", "channel_id asc", 1, 1, []int{2}},
			{"descending window", "channel_id desc", 1, 1, []int{3}},
		} {
			name := "reduced/"
			if compatible {
				name = "compatible/"
			}
			t.Run(name+tc.name, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(ad_order_id INTEGER,advertiser_date TEXT,channel_id INTEGER,bids INTEGER)", "INSERT INTO records VALUES(2678270,'2026-10-14',4,40),(2678270,'2026-10-14',1,10),(2678270,'2026-10-14',3,30),(2678270,'2026-10-14',2,20)"))
				grouped := true
				component := &spec.Component{RootView: &spec.View{Name: "cube", Groupable: &grouped, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}, Source: &spec.ViewSource{SQL: warmupOrderedSQL}}}
				typ := reflect.TypeFor[[]*warmupOrderedRow]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				fields := warmupOrderedFields(compatible)
				view := plan.Root.View
				view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{FieldNames: fields}}
				service, err := (cacheconfig.Config{Identity: "cube", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
				require.NoError(t, err)
				e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: service}})
				require.NoError(t, err)
				_, err = e.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
				require.NoError(t, err)
				selector := xstate.Selector{Fields: warmupOrderedFields(true), OrderBy: tc.order, Limit: tc.limit, Offset: tc.offset}
				before := selector.Clone()
				binder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "cube", Selector: selector}}}
				check := func() {
					t.Helper()
					readCtx := xexec.WithContext(ctx, xexec.New())
					value, err := e.Read(readCtx, &struct{}{}, binder, nil)
					require.NoError(t, err)
					metrics := xexec.GetContext(readCtx).Metrics
					require.Len(t, metrics, 1)
					require.Equal(t, compatible && tc.order == "", metrics[0].Executions[0].CacheStats.FoundWarmup)
					var channels []int
					for _, row := range value.([]*warmupOrderedRow) {
						channels = append(channels, row.ChannelID)
						require.Equal(t, row.ChannelID*10, row.Bids)
						for _, value := range []int{row.Clicks, row.Conversions, row.Impressions, row.TotalSpend, row.V100, row.VStart, row.ViewConversions} {
							require.Equal(t, row.Bids, value)
						}
					}
					require.Equal(t, tc.want, channels)
					require.Equal(t, *before, binder.selectors[0].Selector)
				}
				check()
				// The incompatible canonical entry is bypassed, not caching itself:
				// exact-query replay still preserves this request's ordering/window.
				require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records"))
				check()
				binder.selectors[0].Fields = []string{"ad_order_id", "bids"}
				binder.selectors[0].OrderBy = "channel_id asc"
				_, err = e.Read(ctx, &struct{}{}, binder, nil)
				require.ErrorContains(t, err, "not selected in grouped projection")
			})
		}
	}
}

type warmupOrderedParent struct {
	ID    int                 `sqlx:"id"`
	Items []*warmupOrderedRow `view:"items,selectorProjection=true" on:"ID:id=AdOrderID:ad_order_id"`
}

func TestRelationWarmupOrderingFallsBackSQLite(t *testing.T) {
	for _, compatible := range []bool{false, true} {
		for _, order := range []string{"channel_id asc", "channel_id desc"} {
			name := "reduced/"
			if compatible {
				name = "compatible/"
			}
			t.Run(name+order, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(2678270)", "CREATE TABLE records(ad_order_id INTEGER,advertiser_date TEXT,channel_id INTEGER,bids INTEGER)", "INSERT INTO records VALUES(2678270,'2026-10-14',4,40),(2678270,'2026-10-14',1,10),(2678270,'2026-10-14',3,30),(2678270,'2026-10-14',2,20)"))
				component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}}}
				typ := reflect.TypeFor[[]*warmupOrderedParent]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				view := plan.Root.View.Relations[0].Of.View
				grouped := true
				view.Spec.Groupable = &grouped
				view.Spec.Source = &spec.ViewSource{SQL: warmupOrderedSQL}
				view.Spec.Selector = &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}
				fields := warmupOrderedFields(compatible)
				view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{IndexColumn: "ad_order_id", FieldNames: fields}}
				service, err := (cacheconfig.Config{Identity: "parents/items", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
				require.NoError(t, err)
				e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: service}})
				require.NoError(t, err)
				_, err = e.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}, Request: dexec.ReaderWarmupRequest{View: "items"}})
				require.NoError(t, err)
				selector := xstate.Selector{Fields: warmupOrderedFields(true), OrderBy: order, Limit: 1, Offset: 1}
				before := selector.Clone()
				binder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "items", Selector: selector}}}
				check := func() {
					t.Helper()
					result, err := e.Read(ctx, &struct{}{}, binder, nil)
					require.NoError(t, err)
					parents := result.([]*warmupOrderedParent)
					require.Len(t, parents, 1)
					require.Len(t, parents[0].Items, 1)
					want := 2
					if order == "channel_id desc" {
						want = 3
					}
					require.Equal(t, want, parents[0].Items[0].ChannelID)
					require.Equal(t, want*10, parents[0].Items[0].Bids)
					require.Equal(t, *before, binder.selectors[0].Selector)
				}
				check()
				require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records"))
				check()
				binder.selectors[0].Fields = []string{"ad_order_id", "bids"}
				_, err = e.Read(ctx, &struct{}{}, binder, nil)
				require.ErrorContains(t, err, "not selected in grouped projection")
			})
		}
	}
}
