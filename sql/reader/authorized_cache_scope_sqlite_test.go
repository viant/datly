package reader_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/testutil/sqlfault"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

type cacheScopeInput struct {
	CampaignID    int
	Authorization int
	Scope         string
}

type cacheScopeRow struct {
	OrderID    int     `sqlx:"order_id"`
	CampaignID int     `sqlx:"campaign_id"`
	Spend      float64 `sqlx:"spend"`
}

var errCacheScopeAuthorization = errors.New("authorization lookup failed")

type cacheScopePredicate struct{ input *cacheScopeInput }

func (p *cacheScopePredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	ctx = context.WithValue(ctx, xhandler.InputKey, p.input)
	identity, err := xhandler.CacheIndexIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if identity {
		return nil, nil
	}
	column := "order_id"
	switch p.input.Scope {
	case "error":
		return nil, errCacheScopeAuthorization
	case "denied":
		if err := xhandler.DenyCacheIndex(ctx); err != nil {
			return nil, err
		}
		return &xpredicate.Criteria{Expression: "1=0"}, nil
	case "campaign":
		column = "campaign_id"
	case "conflict":
		if err := xhandler.SelectCacheIndex(ctx, "campaign_id", []any{101}); err != nil {
			return nil, err
		}
	case "invalid":
		if err := xhandler.SelectCacheIndex(ctx, column, []any{[]int{77}}); err != nil {
			return nil, err
		}
	}
	if err := xhandler.SelectCacheIndex(ctx, column, []any{value}); err != nil {
		return nil, err
	}
	return &xpredicate.Criteria{Expression: "r." + column + " = ?", Placeholders: []any{value}}, nil
}

type cacheScopeBinder struct {
	input        *cacheScopeInput
	preparations *atomic.Int32
}

func (b cacheScopeBinder) Bind(_ context.Context, target any, _ ...bindly.BindOption) error {
	if predicate, ok := target.(*cacheScopePredicate); ok {
		predicate.input = b.input
	}
	return nil
}

func (b cacheScopeBinder) parameters() sqlx.ParameterResolver {
	return func(name string) (any, bool, error) {
		return b.input.CampaignID, name == "CampaignID", nil
	}
}

func (b cacheScopeBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	switch key {
	case xhandler.InputKey:
		return b.input, true, nil
	case dexec.ReaderInputPreparerKey:
		return dexec.ReaderInputPreparer(func(_ context.Context, excluded ...string) (*dexec.ReaderInput, error) {
			if len(excluded) != 1 || excluded[0] != "CampaignID" {
				return nil, fmt.Errorf("unexpected identity exclusions %v", excluded)
			}
			b.preparations.Add(1)
			prepared := *b.input
			prepared.CampaignID = 0
			bound := cacheScopeBinder{input: &prepared, preparations: b.preparations}
			return &dexec.ReaderInput{Input: &prepared, Binder: bound, Parameters: bound.parameters()}, nil
		}), true, nil
	}
	return nil, false, nil
}

func TestAuthorizationSelectedUnwarmedIndexFallsBackSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		"CREATE TABLE records(order_id INTEGER,campaign_id INTEGER,spend REAL)",
		"INSERT INTO records VALUES(77,101,12.5),(88,101,40.0),(99,202,60.0)"))
	var queries atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "records") {
			queries.Add(1)
		}
		return nil
	})
	zero := 0
	component := &spec.Component{
		Key:        spec.Key{Kind: spec.KindComponent, Name: "Records"},
		Settings:   &spec.Settings{Cache: &spec.CacheSettings{Warmup: &spec.CacheWarmupSettings{IndexColumn: "campaign_id", IndexParameter: "CampaignID", Limit: &zero}}},
		Parameters: []*spec.Parameter{{Name: "Authorization", Predicates: []*spec.Predicate{{Name: "handler", Auth: true, Args: []string{"fixture.CacheScope"}}}}},
		RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: `SELECT r.order_id,r.campaign_id,r.spend FROM records r
WHERE (:CampaignID=0 OR r.campaign_id=:CampaignID)
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}`}},
	}
	inputType, outputType := reflect.TypeFor[cacheScopeInput](), reflect.TypeFor[[]*cacheScopeRow]()
	predicate, err := readerpredicate.Compile(readerpredicate.CompileInput{Component: component, InputType: inputType, Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[cacheScopePredicate](), nil }})
	require.NoError(t, err)
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewType: outputType, Predicate: predicate})
	require.NoError(t, err)
	native, err := (cacheconfig.Config{Identity: "authorized-scope/records", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: db}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: native}})
	require.NoError(t, err)
	var preparations atomic.Int32
	warmInput := &cacheScopeInput{}
	warmBinder := cacheScopeBinder{input: warmInput, preparations: &preparations}
	_, err = execution.Warmup(t.Context(), dexec.ReaderWarmupInvocation{Input: warmInput, Binder: warmBinder, Parameters: warmBinder.parameters()})
	require.NoError(t, err)
	require.NoError(t, h.ExecStatements(t.Context(), "UPDATE records SET spend=99 WHERE order_id=77", "UPDATE records SET spend=50 WHERE order_id=88"))
	for _, tc := range []struct {
		name, scope string
		authorized  int
		want        []*cacheScopeRow
		warmup      bool
		failure     string
	}{
		{"unwarmed order", "order", 77, []*cacheScopeRow{{77, 101, 99}}, false, ""},
		{"configured campaign", "campaign", 101, []*cacheScopeRow{{77, 101, 12.5}, {88, 101, 40}}, true, ""},
		{"no authorization participation", "", 0, []*cacheScopeRow{{77, 101, 12.5}, {88, 101, 40}}, true, ""},
		{"different unwarmed scope", "order", 88, []*cacheScopeRow{{88, 101, 50}}, false, ""},
		{"explicit denial", "denied", 77, nil, false, ""},
		{"authorization error", "error", 77, nil, false, "authorization lookup failed"},
		{"conflicting selections", "conflict", 77, nil, false, "conflicting cache index selection"},
		{"invalid selection", "invalid", 77, nil, false, "immutable scalar values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queries.Store(0)
			preparations.Store(0)
			input := &cacheScopeInput{CampaignID: 101, Authorization: tc.authorized, Scope: tc.scope}
			binder := cacheScopeBinder{input: input, preparations: &preparations}
			ctx := xexec.WithContext(t.Context(), xexec.New())
			result, err := execution.Read(ctx, input, binder, binder.parameters())
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				require.Zero(t, queries.Load())
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, result.([]*cacheScopeRow))
			metrics := xexec.GetContext(ctx).Metrics
			require.Len(t, metrics, 1)
			require.Equal(t, tc.warmup, metrics[0].Executions[0].CacheStats.FoundWarmup)
			if tc.warmup {
				require.Zero(t, queries.Load())
				require.EqualValues(t, 1, preparations.Load())
			} else {
				require.EqualValues(t, 1, queries.Load())
				require.Zero(t, preparations.Load())
			}
			require.Equal(t, 101, input.CampaignID)
		})
	}
}
