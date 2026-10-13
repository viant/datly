package reader_test

import (
	"context"
	"fmt"
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
	xstate "github.com/viant/xdatly/state"
)

type scalarWarmBucket struct {
	CampaignID int `sqlx:"campaign_id"`
	AccountID  int `sqlx:"account_id"`
	CurrencyID int `sqlx:"currency_id"`
	Bucket     int `sqlx:"bucket"`
	Amount     int `sqlx:"amount"`
}
type scalarWarmParent struct {
	CampaignID int `sqlx:"campaign_id"`
	AccountID  int `sqlx:"account_id"`
	CurrencyID int `sqlx:"currency_id"`
	Buckets    []*scalarWarmBucket
}

func TestCompositeScalarWarmupProjectionWindowSQLite(t *testing.T) {
	for _, parents := range []int{1, 2} {
		for _, mode := range []string{"cold", "lazy", "warm"} {
			t.Run(fmt.Sprintf("%d/%s", parents, mode), func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(campaign_id INTEGER,account_id INTEGER,currency_id INTEGER)", "INSERT INTO parents VALUES(1,2222,1),(1,2222,2)", "CREATE TABLE facts(campaign_id INTEGER,account_id INTEGER,currency_id INTEGER,bucket INTEGER,amount INTEGER)"))
				for bucket := 0; bucket < 55; bucket++ {
					for _, tuple := range [][3]int{{1, 3333, 1}, {1, 2222, 3}, {1, 2222, 1}, {1, 2222, 2}} {
						_, err := h.DB.Exec("INSERT INTO facts VALUES(?,?,?,?,10)", tuple[0], tuple[1], tuple[2], bucket)
						require.NoError(t, err)
					}
				}
				limit, offset := 40, 3
				childSQL := `SELECT f.campaign_id,f.account_id,f.currency_id,f.bucket,f.amount FROM facts f $View.ParentCompositeJoinOn("WHERE","f.campaign_id","f.account_id","f.currency_id") ORDER BY f.bucket`
				child := &spec.View{Name: "buckets", Namespace: "f", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: childSQL, Controls: &spec.ViewControls{Limit: &limit, Offset: &offset}}}
				rootSQL := "SELECT campaign_id,account_id,currency_id FROM parents ORDER BY currency_id"
				if parents == 1 {
					rootSQL += " LIMIT 1"
				}
				links := []*spec.RelationLink{{ParentColumn: "campaign_id", ChildColumn: "campaign_id", ChildField: "CampaignID"}, {ParentColumn: "account_id", ChildColumn: "account_id", ChildField: "AccountID"}, {ParentColumn: "currency_id", ChildColumn: "currency_id", ChildField: "CurrencyID"}}
				component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: rootSQL}, Relations: []*spec.Relation{{Name: "Buckets", Holder: "Buckets", Cardinality: spec.CardinalityMany, View: child, On: links}}}}
				typ := reflect.TypeFor[[]*scalarWarmParent]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				view := plan.Root.View.Relations[0].Of.View
				caches := map[*data.View]cache.Cache{}
				if mode != "cold" {
					settings := &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}
					view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{IndexColumn: "campaign_id"}}
					native, err := (cacheconfig.Config{Identity: "composite/buckets", Settings: settings}).New()
					require.NoError(t, err)
					caches[view] = native
				}
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}, ReadCaches: caches})
				require.NoError(t, err)
				binder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "buckets", Selector: xstate.Selector{Fields: []string{"bucket", "amount"}}}}}
				assert := func() {
					result, err := execution.Read(ctx, &struct{}{}, binder, nil)
					require.NoError(t, err)
					rows := result.([]*scalarWarmParent)
					require.Len(t, rows, parents)
					for _, parent := range rows {
						require.Len(t, parent.Buckets, 40)
						for i, bucket := range parent.Buckets {
							require.Equal(t, i+3, bucket.Bucket)
							require.Equal(t, parent.CampaignID, bucket.CampaignID)
							require.Equal(t, parent.AccountID, bucket.AccountID)
							require.Equal(t, parent.CurrencyID, bucket.CurrencyID)
							require.Equal(t, 10, bucket.Amount)
						}
					}
				}
				if mode == "warm" {
					_, err = execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}, Request: dexec.ReaderWarmupRequest{View: "buckets"}})
					require.NoError(t, err)
				} else {
					assert()
				}
				if mode != "cold" {
					require.NoError(t, h.ExecStatements(ctx, "DROP TABLE facts"))
					assert()
				}
				require.Equal(t, childSQL, child.Source.SQL)
			})
		}
	}
}
