package reader_test

import (
	"context"
	"encoding/json"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	xresponse "github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestHTTPMetricsHeadersPreserveNativeSQLSQLite(t *testing.T) {
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
		t.Fatal(err)
	}
	probe := &readerExportProbe{}
	f := typedGraphFixture{db: db, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "HTTPObserved"}, Name: "HTTPObserved", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}, output: reflect.TypeOf(observationOutput{})}
	app, _, err := f.compile(runtime.WithObservability(runtime.ObservabilityConfig{Logger: quietObservationLog{}, OTel: &otel.Config{Enabled: true, Exporter: probe}}))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/graph?password=private", nil)
	request.Header.Set("Authorization", "Bearer private-JWT")
	request.Header.Set("Datly-Show-Metrics", "true")
	response := httptest.NewRecorder()
	handler, err := (gateway.Config{Metrics: &gateway.MetricsConfig{}}).NewHandler(app, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var header xresponse.Metric
	if err := json.Unmarshal([]byte(response.Header().Get("Datly-Metrics-Records")), &header); err != nil {
		t.Fatal(err)
	}
	if len(header.Executions) != 1 || header.Executions[0].SQL != "" {
		t.Fatal("non-debug header SQL exposed")
	}
	var body observationOutput
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Metrics) != 1 || !strings.Contains(body.Metrics[0].Executions[0].SQL, "SELECT") {
		t.Fatal("header hiding mutated native metrics output")
	}
	if err = app.Observability().Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if app.Observability().ExportStats().Exported != 1 {
		t.Fatal("completed HTTP record not exported")
	}
}
