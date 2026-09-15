package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/spend"
)

const spendCube = `{"dimensions":{"accountID":true,"region":true},"measures":{"totalSpend":true},"filters":{"tenant":"acme","channel":"web"}}`
const spendCompose = `{"cubes":[{"filters":{"tenant":"acme","channel":"web"}},{"inheritFrom":1,"filters":{"channel":"store"}}],"sql":"SELECT t1.customer_id AS customer, t1.spend_total AS web, COALESCE(t2.spend_total, 0) AS store FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.customer_id = t2.customer_id ORDER BY t1.customer_id"}`
const spendComposeMultiKey = `{"cubes":[{"filters":{"tenant":"acme","channel":"web"}},{"inheritFrom":1,"filters":{"channel":"store"}}],"sql":"SELECT t1.customer_id AS customer, t1.region_code AS region, t1.spend_total AS web, COALESCE(t2.spend_total, 0) AS store FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.customer_id = t2.customer_id AND t1.region_code = t2.region_code ORDER BY t1.customer_id, t1.region_code"}`

func newStandaloneReportFixture(t *testing.T, options ...func(*fixture.Fixture, *config.Config)) (*fixture.Fixture, *config.Config, *testharness.JWT) {
	t.Helper()
	f := fixture.New(t)
	require.NoError(t, f.DB.ExecStatements(context.Background(),
		`CREATE TABLE report_access(tenant TEXT)`,
		`INSERT INTO report_access VALUES ('acme')`,
		`CREATE TABLE report_spend(tenant TEXT,account_id INTEGER,region TEXT,channel TEXT,amount REAL)`,
		`INSERT INTO report_spend VALUES ('acme',1,'EU','web',100),('acme',1,'EU','web',50),('acme',1,'US','web',1000),('acme',1,'EU','store',2000),('acme',2,'EU','web',70),('other',1,'EU','web',9000)`))
	f.WriteConfig(t, func(c map[string]any) {
		c["Info"] = map[string]any{"title": "Standalone reports", "version": "1"}
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/spend"}}
	})
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	require.NoError(t, err)
	jwt := testharness.NewJWT(t)
	cfg.JWTValidator = jwt.Config()
	for _, option := range options {
		option(f, cfg)
	}
	return f, cfg, jwt
}

func newStandaloneReport(t *testing.T, options ...func(*fixture.Fixture, *config.Config)) (*Server, *fixture.Fixture, string) {
	t.Helper()
	f, cfg, jwt := newStandaloneReportFixture(t, options...)
	s, err := New(context.Background(), Options{Config: cfg, Registry: spend.Exports()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Shutdown(context.Background())) })
	require.NoError(t, s.Reload(context.Background(), 1))
	return s, f, jwt.Sign(t, "reader", time.Now().Add(time.Hour))
}

func TestStandaloneDiscoveredReportsSQLite(t *testing.T) {
	s, f, token := newStandaloneReport(t)
	call := func(path, body, key, jwt string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Report-Key", key)
		r.Header.Set("Authorization", jwt)
		r.Header.Set("X-Tenant", "other") // frame filter overrides the outer request
		r.Header.Set("Cookie", "channel=store")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	for _, tc := range []struct{ name, path, body string }{
		{"cube", "/cube", spendCube}, {"compose", "/cube/compose", spendCompose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, credentials := range []struct {
				name, key, jwt string
				status         int
			}{
				{"missing key", "", token, 403}, {"wrong key", "wrong", token, 403},
				{"missing JWT", "report-secret", "", 401}, {"bad JWT", "report-secret", "Bearer invalid", 401},
				{"valid", "report-secret", token, 200},
			} {
				t.Run(credentials.name, func(t *testing.T) {
					w := call("/secure"+tc.path, tc.body, credentials.key, credentials.jwt)
					require.Equal(t, credentials.status, w.Code, w.Body.String())
					if w.Code == 200 {
						assertStandaloneReport(t, w.Body.Bytes(), tc.name, 150)
					}
				})
			}
			w := call("/spend"+tc.path, tc.body, "", "")
			require.Equal(t, 200, w.Code, w.Body.String())
			assertStandaloneReport(t, w.Body.Bytes(), tc.name, 150)
			if tc.name == "compose" {
				w = call("/spend"+tc.path, spendComposeMultiKey, "", "")
				require.Equal(t, 200, w.Code, w.Body.String())
				assertStandaloneReport(t, w.Body.Bytes(), "composeMultiKey", 150)
			}
			missing := strings.ReplaceAll(tc.body, `"tenant":"acme",`, "")
			if tc.name == "cube" {
				// Omitted ordinary cube filters retain source binding semantics.
				// Without either a filter or its source header, required fails.
				r := httptest.NewRequest("POST", "/spend"+tc.path, strings.NewReader(missing))
				r.Header.Set("Content-Type", "application/json")
				w = httptest.NewRecorder()
				s.ServeHTTP(w, r)
			} else {
				// Composition deliberately masks ambient values per frame.
				w = call("/spend"+tc.path, missing, "", "")
			}
			require.NotEqual(t, 200, w.Code, "required source tenant was lost")
		})
	}
	// Exact SQL aliases are public composition names; inferred Go names are not aliases.
	bad := strings.ReplaceAll(spendCompose, "t1.customer_id", "t1.AccountID")
	w := call("/spend/cube/compose", bad, "", "")
	require.NotEqual(t, 200, w.Code, w.Body.String())
	metadata, err := s.Metadata(context.Background())
	require.NoError(t, err)
	require.Len(t, metadata.Components, 6)
	for _, component := range metadata.Components {
		if strings.HasSuffix(component.Name, "Cube") || strings.HasSuffix(component.Name, "Compose") {
			require.Nil(t, component.RootView, "derived output tags must not create a second reader")
		} else {
			require.NotNil(t, component.RootView)
			require.True(t, strings.HasSuffix(component.RootView.Source.URI, "queries/spend.sql"))
			require.Empty(t, component.RootView.Source.SQL, "authored URI ownership changed")
		}
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/v1/api/meta/openapi", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "/spend/cube/compose")
	// The unchanged source reader remains exposed and uses its original resource SQL.
	r := httptest.NewRequest("GET", "/spend", nil)
	r.Header.Set("X-Tenant", "acme")
	r.Header.Set("Cookie", "channel=web")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	require.Equal(t, 200, w.Code, w.Body.String())
	assertStandaloneReport(t, w.Body.Bytes(), "cube", 150)
	query := filepath.Join(f.Root, "spend/queries/spend.sql")
	sql, err := os.ReadFile(query)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(query, []byte(strings.ReplaceAll(string(sql), "SUM(s.amount)", "2 * SUM(s.amount)")), 0600))
	require.NoError(t, s.Reload(context.Background(), 2))
	for _, tc := range []struct{ name, path, body string }{{"cube", "/spend/cube", spendCube}, {"compose", "/spend/cube/compose", spendCompose}} {
		w = call(tc.path, tc.body, "", "")
		require.Equal(t, 200, w.Code, w.Body.String())
		assertStandaloneReport(t, w.Body.Bytes(), tc.name, 300)
	}
	require.NoError(t, os.Remove(query))
	require.Error(t, s.Reload(context.Background(), 3))
	require.Equal(t, uint64(2), s.manager.Revision())
	w = call("/spend/cube", spendCube, "", "")
	require.Equal(t, 200, w.Code, w.Body.String())
	assertStandaloneReport(t, w.Body.Bytes(), "cube", 300)
	// Every derived invocation still resolves the source's independently scoped
	// view provider. A missing dependency cannot be hidden by report registration.
	require.NoError(t, f.DB.ExecStatements(context.Background(), "DROP TABLE report_access"))
	w = call("/spend/cube", spendCube, "", "")
	require.NotEqual(t, 200, w.Code, w.Body.String())
	require.NoError(t, s.Shutdown(context.Background()))
	require.Error(t, s.source.connections.SQL.DB.Ping())
	require.NoError(t, f.DB.DB.Ping())

}

func assertStandaloneReport(t *testing.T, body []byte, kind string, eu float64) {
	t.Helper()
	if kind == "cube" {
		var result struct {
			Rows []spend.Row `json:"rows"`
		}
		require.NoError(t, json.Unmarshal(body, &result))
		require.Len(t, result.Rows, 3, string(body))
		values := map[string]float64{}
		for _, row := range result.Rows {
			if row.AccountID == 1 {
				values[row.Region] = row.TotalSpend
			}
		}
		require.Equal(t, eu, values["EU"], string(body))
		require.Equal(t, eu/150*1000, values["US"], string(body))
	} else {
		var result struct {
			Data []struct {
				Customer   int
				Region     string
				Web, Store float64
			}
		}
		require.NoError(t, json.Unmarshal(body, &result))
		if kind == "composeMultiKey" {
			require.Len(t, result.Data, 3, string(body))
			values := map[string]struct{ Web, Store float64 }{}
			for _, row := range result.Data {
				values[fmt.Sprintf("%d:%s", row.Customer, row.Region)] = struct{ Web, Store float64 }{row.Web, row.Store}
			}
			require.Equal(t, struct{ Web, Store float64 }{eu, eu / 150 * 2000}, values["1:EU"], string(body))
			require.Equal(t, struct{ Web, Store float64 }{eu / 150 * 1000, 0}, values["1:US"], string(body))
			require.Equal(t, struct{ Web, Store float64 }{eu / 150 * 70, 0}, values["2:EU"], string(body))
			return
		}
		require.Len(t, result.Data, 2, string(body))
		require.Equal(t, 1, result.Data[0].Customer)

		require.Equal(t, eu/150*1150, result.Data[0].Web)
		require.Equal(t, eu/150*2000, result.Data[0].Store)
		require.Equal(t, float64(0), result.Data[1].Store)
	}
}
