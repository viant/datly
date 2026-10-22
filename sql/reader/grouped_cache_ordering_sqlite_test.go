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
	xstate "github.com/viant/xdatly/state"
)

func TestGroupedCacheOrderingOutsideWarmupFallsBackSQLite(t *testing.T) {
	type row struct {
		AccountID int `sqlx:"account_id"`
		ChannelID int `sqlx:"channel_id"`
		Total     int `sqlx:"total"`
	}
	type output struct{ Rows []*row }
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE performance(account_id INTEGER, channel_id INTEGER, amount INTEGER)", "INSERT INTO performance VALUES (1,2,20),(1,1,10),(1,1,5)"))
	groupable := true
	policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}
	component := &spec.Component{RootView: &spec.View{Name: "performance", Groupable: &groupable, Selector: policy, Source: &spec.ViewSource{SQL: "SELECT p.account_id, p.channel_id, SUM(p.amount) AS total FROM performance p GROUP BY p.account_id, p.channel_id"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
	require.NoError(t, err)
	view := plan.Root.View
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{FieldNames: []string{"account_id", "total"}}}
	native, err := (cacheconfig.Config{Identity: "grouped-ordering", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: native}})
	require.NoError(t, err)
	groups, err := execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	require.Equal(t, 1, groups)
	for _, tc := range []struct {
		name   string
		fields []string
		order  string
		deny   string
	}{
		{"selected dimension outside warmup", []string{"account_id", "channel_id", "total"}, "channel_id ASC", ""},
		{"selected dimension ordinal outside warmup", []string{"account_id", "channel_id", "total"}, "2 ASC", ""},
		{"selected measure ordinal beyond warmup width", []string{"account_id", "channel_id", "total"}, "3 ASC", ""},
		{"unselected dimension", []string{"account_id", "total"}, "channel_id ASC", "not selected in grouped projection"},
		{"unknown order", []string{"account_id", "channel_id", "total"}, "missing", "not in source projection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := xstate.Selector{Fields: tc.fields, OrderBy: tc.order, Limit: 1, Offset: 1}
			before := selected.Clone()
			result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "performance", Selector: selected}}}, nil)
			if tc.deny != "" {
				require.ErrorContains(t, err, tc.deny)
				return
			}
			require.NoError(t, err)
			rows := result.Data.(*output).Rows
			require.Len(t, rows, 1)
			require.Equal(t, &row{AccountID: 1, ChannelID: 2, Total: 20}, rows[0])
			require.Equal(t, before, &selected)
		})
	}
}
