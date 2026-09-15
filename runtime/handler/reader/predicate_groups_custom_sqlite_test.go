package reader

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/auth"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/scy/auth/jwt"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

// Both predicates are resolved by their linked full Go type identity.
// DQL declares the imports; no application predicate-name registry is installed.
type orderScopePredicate struct {
	Input   *OrderSearchInput `bind:"kind=input,required"`
	Ceiling int               `bind:"kind=ceiling,required"`
}

var _ xpredicate.Handler = (*orderScopePredicate)(nil)

func (p *orderScopePredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	claims, ok := value.(*jwt.Claims)
	if !ok || claims == nil || p.Input == nil || p.Input.JWT != claims || claims.Subject == "" {
		return nil, fmt.Errorf("verified authorization input required")
	}
	if claims.Subject == "denied" {
		return nil, fmt.Errorf("order access denied")
	}
	return &xpredicate.Criteria{Expression: "r.owner_subject = ? AND r.total <= ?", Placeholders: []any{claims.Subject, p.Ceiling}}, nil
}

type exactReferencePredicate struct {
	Enabled bool `bind:"kind=referencePolicy,required"`
}

var _ xpredicate.Handler = (*exactReferencePredicate)(nil)

func (p *exactReferencePredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reference, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("reference must be a string")
	}
	if !p.Enabled {
		return nil, fmt.Errorf("reference filtering disabled")
	}
	// Explicit application policy: "all" and present-empty mean no refinement.
	if reference == "all" {
		return nil, nil
	}
	if reference == "" {
		return &xpredicate.Criteria{}, nil
	}
	return &xpredicate.Criteria{Expression: "r.reference = ?", Placeholders: []any{reference}}, nil
}

func TestReaderCustomAndBuiltinPredicateGroupsDQLSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(ctx,
		`CREATE TABLE purchase_orders (id INTEGER PRIMARY KEY,tenant_id INTEGER,workspace_id INTEGER,owner_subject TEXT,name TEXT,reference TEXT,total INTEGER)`,
		`INSERT INTO purchase_orders VALUES
         (1,7,70,'alice','Alpha desk','D-1',-5),
         (2,7,70,'alice','Beta chair','Alpha-2',0),
         (3,7,70,'alice','Alpha lamp','L-3',20),
         (4,7,70,'alice','Alpha over ceiling','L-4',50),
         (5,8,70,'alice','Alpha other tenant','Alpha-5',10),
         (6,7,71,'alice','Alpha other workspace','Alpha-6',10),
         (7,7,70,'bob','Alpha other owner','Alpha-7',10),
         (8,7,70,'alice','Alpha quote','x'' OR 1=1 --',30)`))
	credentials := testharness.NewJWT(t)
	factory, err := auth.New(ctx, &auth.Config{JWTValidator: credentials.Config()})
	require.NoError(t, err)
	valid := credentials.Sign(t, "alice", time.Now().Add(time.Hour))
	bob := credentials.Sign(t, "bob", time.Now().Add(time.Hour))
	denied := credentials.Sign(t, "denied", time.Now().Add(time.Hour))
	expired := credentials.Sign(t, "alice", time.Now().Add(-time.Hour))
	wrongSignature := testharness.NewJWT(t).Sign(t, "alice", time.Now().Add(time.Hour))
	source, err := os.ReadFile("testdata/predicate_groups_custom.sql")
	require.NoError(t, err)
	for _, mode := range []string{"WHERE", "AND"} {
		t.Run(mode, func(t *testing.T) {
			dql := string(source)
			if mode == "AND" {
				dql = strings.Replace(dql, `    ${predicate.Builder()`, `    WHERE r.id > 0 ${predicate.Builder()`, 1)
				dql = strings.Replace(dql, `Build("WHERE")`, `Build("AND")`, 1)
			}
			types := testTypeCatalog(t, map[string]reflect.Type{
				"example.com/app/orders/read.OrderSearchInput":   reflect.TypeOf(OrderSearchInput{}),
				"example.com/app/orders/read.OrderSearchOutput":  reflect.TypeOf(OrderSearchOutput{}),
				"example.com/app/orders/read.Order":              reflect.TypeOf(Order{}),
				"github.com/viant/scy/auth/jwt.Claims":           reflect.TypeOf(jwt.Claims{}),
				"example.com/app/orders/security.OrderScope":     reflect.TypeOf(orderScopePredicate{}),
				"example.com/app/orders/security.ExactReference": reflect.TypeOf(exactReferencePredicate{}),
			})
			compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Name: "orders", Text: dql, Types: types})
			require.NoError(t, err)
			artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeOf(OrderSearchInput{}), OutputType: reflect.TypeOf(OrderSearchOutput{}), Types: types, CodecFactory: factory})
			require.NoError(t, err)
			trace := &predicateGroupsTrace{Evaluator: artifact.Reader.Root.Template}
			artifact.Reader.Root.Template = trace
			for _, tc := range []struct {
				name, query, credential                                         string
				ids                                                             []int
				args                                                            []any
				missingCeiling, missingReferencePolicy, disabledReferencePolicy bool
				fail, beforeTemplate                                            bool
			}{
				{name: "optional groups absent skips custom dependency", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70}, missingReferencePolicy: true},
				{name: "builtin OR remains under custom scope", query: "q=Alpha", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70, "%Alpha%", "%Alpha%"}},
				{name: "custom and builtin in bounds group", query: "q=Alpha&min=0&max=20&ref=Alpha-2", credential: valid, ids: []int{2}, args: []any{"alice", 30, 7, 70, "%Alpha%", "%Alpha%", 0, 20, "Alpha-2"}},
				{name: "zero custom plus builtin bounds", query: "max=0", credential: valid, ids: []int{1, 2}, args: []any{"alice", 30, 7, 70, 0}},
				{name: "custom value is bound data", query: "ref=" + url.QueryEscape("x' OR 1=1 --"), credential: valid, ids: []int{8}, args: []any{"alice", 30, 7, 70, "x' OR 1=1 --"}},
				{name: "nil criteria omits optional refinement", query: "ref=all", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70}},
				{name: "empty criteria omits optional refinement", query: "ref=", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70}},
				{name: "verified different owner", query: "q=Alpha", credential: bob, ids: []int{7}, args: []any{"bob", 30, 7, 70, "%Alpha%", "%Alpha%"}},
				{name: "no authority from query claims", query: "JWT=bob&owner=bob&tenant=8&q=Alpha", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70, "%Alpha%", "%Alpha%"}},
				{name: "missing credential", fail: true, beforeTemplate: true},
				{name: "wrong signature", credential: wrongSignature, fail: true, beforeTemplate: true},
				{name: "expired credential", credential: expired, fail: true, beforeTemplate: true},
				{name: "business authorization denial", credential: denied, fail: true},
				{name: "missing required scope dependency", credential: valid, missingCeiling: true, fail: true},
				{name: "present custom filter requires dependency", query: "ref=D-1", credential: valid, missingReferencePolicy: true, fail: true},
				{name: "custom Compute error", query: "ref=D-1", credential: valid, disabledReferencePolicy: true, fail: true},
				{name: "empty groups after prior invocations", credential: valid, ids: []int{1, 2, 3, 8}, args: []any{"alice", 30, 7, 70}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query, err := url.ParseQuery(tc.query)
					require.NoError(t, err)
					providers := []locator.Provider{
						handlerprovider.Named("tenant", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
							return 7, name == "tenant", nil
						}),
						handlerprovider.Named("workspace", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
							return 70, name == "workspace", nil
						}),
					}
					if !tc.missingCeiling {
						providers = append(providers, handlerprovider.Static(xhandler.ValueKey("ceiling"), 30))
					}
					if !tc.missingReferencePolicy {
						providers = append(providers, handlerprovider.Static(xhandler.ValueKey("referencePolicy"), !tc.disabledReferencePolicy))
					}
					before := trace.calls
					actual, err := NewService().Read(ctx, &Session{Component: artifact.Component, OutputType: reflect.TypeOf(OrderSearchOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}, Providers: providers, Scope: testharness.NewRequest(http.MethodGet, "/orders/search").WithQuery(query).WithHeaders(http.Header{"Authorization": {tc.credential}})})
					if tc.fail {
						require.Error(t, err)
						require.Nil(t, actual)
						if tc.beforeTemplate {
							require.Equal(t, before, trace.calls)
						} else {
							require.Equal(t, before+1, trace.calls)
							require.Empty(t, trace.result.SQL, "failed predicate must not produce executable protected SQL")
						}
						return
					}
					require.NoError(t, err)
					require.Equal(t, before+1, trace.calls)
					var ids []int
					for _, row := range actual.(*OrderSearchOutput).Data {
						require.Equal(t, 7, row.TenantID)
						require.Equal(t, 70, row.WorkspaceID)
						ids = append(ids, row.ID)
					}
					require.Equal(t, tc.ids, ids)
					require.Equal(t, tc.args, trace.result.Args)
					require.Equal(t, len(tc.args), strings.Count(trace.result.SQL, "?"))
					require.NotContains(t, trace.result.SQL, "alice")
					require.NotContains(t, trace.result.SQL, "bob")
					require.NotContains(t, trace.result.SQL, "x' OR 1=1 --")
					require.NotNil(t, trace.input.JWT)
					require.Equal(t, query.Has("ref"), trace.input.Has.Reference)
				})
			}
		})
	}
}
