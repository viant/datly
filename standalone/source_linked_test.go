package standalone

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/handlerreport"
	_ "github.com/viant/datly/standalone/testdata/app/records"
	"github.com/viant/datly/standalone/testdata/app/selectorpolicy"
	"github.com/viant/datly/standalone/testdata/linkeddefault"
)

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func TestStandaloneLinkedOnlyUsesConfiguredDefaultConnector(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{"github.com/viant/datly/standalone/testdata/linkeddefault"}, "LinkedOnly": true}
	})
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
	_ = linkeddefault.LinkedType
	server, err := New(ctx, Options{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	require.NoError(t, server.Reload(ctx, 1))
	request := httptest.NewRequest("POST", "/linked-default", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"ready":true`)
}

func TestStandaloneLinkedOnlyDerivedReportWithoutSourceDirectory(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	require.NoError(t, f.DB.ExecStatements(ctx,
		"CREATE TABLE report_facts(country TEXT, region TEXT, site_id INTEGER, amount INTEGER, enabled INTEGER)",
		"INSERT INTO report_facts VALUES ('US','MA',1,2,1),('US','MA',2,3,1)",
	))
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/handlerreport"}, "LinkedOnly": true}
	})
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
	_ = handlerreport.LinkedDatlyType
	diagnostics := &synchronizedBuffer{}
	server, err := New(ctx, Options{Config: cfg, Diagnostics: diagnostics})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	require.NoError(t, server.Reload(ctx, 1))
	require.NotContains(t, diagnostics.text(), "datly bootstrap linked materialize")
	request := httptest.NewRequest("POST", "/native-report/cube", strings.NewReader(`{"dimensions":{"country":true},"measures":{"amount":true},"filters":{"permit":true}}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"amount":5`)
	require.Equal(t, 1, strings.Count(diagnostics.text(), "datly bootstrap linked materialize"))
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *synchronizedBuffer) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func TestStandaloneLinkedOnlyLazyWithoutSourceDirectory(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/records"}, "LinkedOnly": true}
	})
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
	cfg.Const, err = constant.New(map[string]string{"Environment": "test"})
	require.NoError(t, err)
	diagnostics := &synchronizedBuffer{}
	server, err := New(ctx, Options{Config: cfg, Diagnostics: diagnostics, RequireLinked: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	require.NoError(t, server.Reload(ctx, 1))
	require.NotContains(t, diagnostics.text(), "datly bootstrap linked materialize")

	const parallel = 12
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest("GET", "/records/1", nil))
			if response.Code != 200 || !strings.Contains(response.Body.String(), "first") {
				t.Errorf("linked read status=%d body=%s", response.Code, response.Body.String())
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, strings.Count(diagnostics.text(), "datly bootstrap linked materialize"))

	missing := httptest.NewRecorder()
	server.ServeHTTP(missing, httptest.NewRequest("GET", "/missing", nil))
	require.Equal(t, 404, missing.Code)
	require.Equal(t, 1, strings.Count(diagnostics.text(), "datly bootstrap linked materialize"))
}

func TestStandaloneLinkedOnlyConcurrentDistinctComponents(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/selectorpolicy"}, "LinkedOnly": true}
	})
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
	_ = selectorpolicy.DatlyLinkedType
	diagnostics := &synchronizedBuffer{}
	server, err := New(ctx, Options{Config: cfg, Diagnostics: diagnostics})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	require.NoError(t, server.Reload(ctx, 1))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		path := "/selector-allowed"
		if i%2 == 0 {
			path = "/selector-inferred"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
			if response.Code != 200 {
				t.Errorf("concurrent linked %s status=%d body=%s", path, response.Code, response.Body.String())
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 2, strings.Count(diagnostics.text(), "datly bootstrap linked materialize"))
}
