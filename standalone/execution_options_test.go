package standalone

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/executionoptions"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/sqlx/metadata/database"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

var executionHintStatements struct {
	sync.Mutex
	sql []string
}

var executionOptionsDriverOnce sync.Once

type executionOptionsDriver struct{}
type executionOptionsConn struct{ driver.Conn }

func (executionOptionsDriver) Open(name string) (driver.Conn, error) {
	conn, err := (&sqlite3.SQLiteDriver{ConnectHook: func(c *sqlite3.SQLiteConn) error {
		if err := c.RegisterFunc("concat", func(parts ...string) string { return strings.Join(parts, "") }, true); err != nil {
			return err
		}
		return c.RegisterFunc("version", func() string { return "8.0.36" }, true)
	}}).Open(name)
	if err != nil {
		return nil, err
	}
	return &executionOptionsConn{Conn: conn}, nil
}

func (c *executionOptionsConn) Prepare(query string) (driver.Stmt, error) {
	executionHintStatements.Lock()
	executionHintStatements.sql = append(executionHintStatements.sql, query)
	executionHintStatements.Unlock()
	// Source-indexed bootstrap performs real column discovery. Supply the
	// fixture's fixed MySQL metadata while still discovering SELECT output
	// columns through SQLite's actual prepared statements below.
	lower := strings.ToLower(query)
	if strings.Contains(lower, "information_schema.processlist") {
		query = "SELECT '1' AS PID,'fixture' AS USER_NAME,'' AS CATALOG,'fixture' AS SCHEMA_NAME,'' AS APP_NAME"
	} else if strings.Contains(lower, "information_schema.columns") {
		args := strings.Count(query, "?")
		query = `SELECT '' TABLE_CATALOG,'fixture' TABLE_SCHEMA,'records' TABLE_NAME,'id' COLUMN_NAME,1 ORDINAL_POSITION,'' COLUMN_COMMENT,'int' DATA_TYPE,0 CHARACTER_MAXIMUM_LENGTH,0 NUMERIC_PRECISION,0 NUMERIC_SCALE,'NO' IS_NULLABLE,NULL COLUMN_DEFAULT,'' COLUMN_KEY,NULL IS_AUTOINCREMENT
UNION ALL SELECT '','fixture','records','name',2,'','text',0,0,0,'YES',NULL,'',NULL`
		if args > 0 {
			query = "SELECT * FROM (" + query + ") metadata WHERE " + strings.TrimSuffix(strings.Repeat("? IS NOT NULL AND ", args), " AND ")
		}
	} else if strings.Contains(lower, "information_schema.table_constraints") {
		args := strings.Count(query, "?")
		query = "SELECT '' CONSTRAINT_NAME,'' CONSTRAINT_TYPE,'' CONSTRAINT_CATALOG,'' CONSTRAINT_SCHEMA,'' TABLE_NAME,'' COLUMN_NAME,0 ORDINAL_POSITION,0 POSITION_IN_UNIQUE_CONSTRAINT,'' ON_UPDATE,'' ON_DELETE,'' ON_MATCH,'' REFERENCED_TABLE_NAME,'' REFERENCED_COLUMN_NAME,'' REFERENCED_TABLE_SCHEMA WHERE 0"
		query += strings.Repeat(" AND ? IS NOT NULL", args)
	}
	return c.Conn.Prepare(query)
}

func TestExecutionHintsHTTPMCPBootstrapModes(t *testing.T) {
	const driverName = "datly_execution_options_fixture"
	executionOptionsDriverOnce.Do(func() { sql.Register(driverName, executionOptionsDriver{}) })
	typ := reflect.TypeFor[executionOptionsDriver]()
	require.NoError(t, registry.RegisterDriver(database.Product{Name: "MySQL", Major: 8}, typ.PkgPath(), typ.Name()))
	for _, mode := range []string{"eager", "indexed", "linked"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx, "INSERT INTO records VALUES(2,'other')"))
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/executionoptions"}, "EagerComponents": mode == "eager", "LinkedOnly": mode == "linked"}
				c["Connectors"] = []any{map[string]any{"Name": "main", "Driver": driverName, "DSN": f.DSN}}
				c["Caches"] = map[string]any{"hinted": map[string]any{"Enabled": true, "Provider": "afs", "Location": t.TempDir(), "TTL": "1m"}}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			if mode == "linked" {
				cfg.BaseDir = filepath.Join(t.TempDir(), "source-unavailable")
			}
			_ = executionoptions.LinkedType
			server, err := New(ctx, Options{Config: cfg})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
			require.NoError(t, server.Reload(ctx, 1))
			executionHintStatements.Lock()
			executionHintStatements.sql = nil
			executionHintStatements.Unlock()
			http := httptest.NewRecorder()
			server.ServeHTTP(http, httptest.NewRequest("GET", "/execution-hints/1", nil))
			if http.Code != 200 {
				_, nativeErr := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/executionoptions", Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/execution-hints/{id}"}}, Input: &executionoptions.Input{ID: 1}})
				require.NoError(t, nativeErr, "unmasked reader failure")
			}
			require.Equal(t, 200, http.Code, http.Body.String())
			require.JSONEq(t, `{"rows":[{"id":1,"name":"first"}]}`, http.Body.String())
			executionHintStatements.Lock()
			statements := append([]string(nil), executionHintStatements.sql...)
			executionHintStatements.Unlock()
			found := false
			for _, text := range statements {
				found = found || strings.Contains(text, "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=off') */")
			}
			require.True(t, found, "actual database preparation must contain the hint: %v", statements)
			second := httptest.NewRecorder()
			server.ServeHTTP(second, httptest.NewRequest("GET", "/execution-hints/2", nil))
			require.Equal(t, 200, second.Code, second.Body.String())
			require.JSONEq(t, `{"rows":[{"id":2,"name":"other"}]}`, second.Body.String())
			// The real MCP path must replay the same exact-query lazy entry.
			require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE records"))
			native := (mcpclient.Config{Source: server.manager}).New(t)
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "HintedRead", Arguments: map[string]any{"id": 1}})
			require.NoError(t, err)
			if result.IsError != nil {
				require.False(t, *result.IsError)
			}
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, http.Body.String(), string(encoded))
		})
	}
}
