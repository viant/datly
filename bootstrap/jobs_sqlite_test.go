package bootstrap

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/jobs"
	dsql "github.com/viant/datly/sql"
	xasync "github.com/viant/xdatly/async"
	"strings"
	"testing"
	"time"
)

func TestJobStoreNamedConnectorDatasetSQLite(t *testing.T) {
	ctx := context.Background()
	app, job := sqlite.New(t), sqlite.New(t)
	if err := app.ExecStatements(ctx, sqlite.DatlyJobsSchema); err != nil {
		t.Fatal(err)
	}
	connectors := &dsql.SQLComponent{DB: app.DB}
	if err := connectors.RegisterConnector("app", app.DB); err != nil {
		t.Fatal(err)
	}
	if err := connectors.RegisterConnector("jobs", job.DB); err != nil {
		t.Fatal(err)
	}
	store, err := (JobStoreConfig{SQL: connectors, Connector: "jobs", Dataset: "main"}).NewStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := &jobs.Record{Job: xasync.Job{ID: "named", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/items"}, CreationTime: time.Now().UTC()}, State: `{}`}
	if err := store.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := app.DB.QueryRow(`SELECT count(*) FROM DATLY_JOBS`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("wrong connector count=%d err=%v", count, err)
	}
	if _, err := store.Get(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := (JobStoreConfig{SQL: connectors, Connector: "absent"}).NewStore(ctx); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unknown connector=%v", err)
	}
	if _, err := (JobStoreConfig{SQL: connectors, Connector: "jobs", Table: "absent", DisableTableCreation: true}).NewStore(ctx); err == nil {
		t.Fatal("missing original job table accepted")
	}
}
