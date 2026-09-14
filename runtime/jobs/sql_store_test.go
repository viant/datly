package jobs_test

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
	"github.com/viant/xdatly/async/destination"
	"reflect"
	"testing"
	"time"
)

func TestOriginalColumnsRoundTripSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	store, err := jobs.NewSQLStore(context.Background(), jobs.SQLConfig{DB: h.DB})
	if err != nil {
		t.Fatal(err)
	}
	text := "value"
	create := destination.CreateDisposition("CREATE_IF_NEEDED")
	write := destination.WriteDisposition("WRITE_APPEND")
	record := &jobs.Record{Job: xasync.Job{
		ID: "original", MatchKey: "match", Status: xasync.StatusPending,
		Table:   destination.Table{Connector: &text, TableName: &text, TableDataset: &text, TableSchema: &text, CreateDisposition: &create, Template: &text, WriteDisposition: &write},
		Cache:   destination.Cache{Cache: &text, CacheKey: &text, CacheSet: &text, CacheNamespace: &text},
		Request: xasync.Request{Method: "GET", URI: "/items"}, Principal: xasync.Principal{UserID: &text, UserEmail: &text},
		MainView: "items", Module: "mod", Labels: "label", JobType: "reader", EventURL: "event", CreationTime: time.Now().UTC().Truncate(time.Microsecond),
	}, State: `{"ID":7}`, Metrics: `{}`, SQLQuery: `[{"Query":"SELECT ?","Args":[7]}]`}
	now := record.CreationTime
	failure := "stored error"
	record.StartTime, record.EndTime, record.ExpiryTime = &now, &now, &now
	record.WaitTimeInMcs, record.RunTimeInMcs = 123456789, 987654321
	record.Error, record.Deactivated = &failure, true
	if err := store.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	actual, err := store.Get(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record, actual) {
		t.Fatalf("original column mapping differs:\n%#v\n%#v", record, actual)
	}
	// Independently check the original service/jobs/service.go column inventory.
	expected := map[string]bool{}
	for _, name := range []string{"ID", "MatchKey", "Status", "Metrics", "Connector", "TableName", "TableDataset", "TableSchema", "CreateDisposition", "Template", "WriteDisposition", "Cache", "CacheKey", "CacheSet", "CacheNamespace", "Method", "URI", "State", "UserEmail", "UserID", "MainView", "Module", "Labels", "JobType", "EventURL", "Error", "CreationTime", "StartTime", "EndTime", "ExpiryTime", "WaitTimeInMcs", "RunTimeInMcs", "SQLQuery", "Deactivated"} {
		expected[name] = true
	}
	rows, err := h.DB.QueryContext(ctx, `PRAGMA table_info(DATLY_JOBS)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var index, notnull, pk int
		var name, kind string
		var fallback any
		if err := rows.Scan(&index, &name, &kind, &notnull, &fallback, &pk); err != nil {
			t.Fatal(err)
		}
		if !expected[name] {
			t.Errorf("alternative column %s", name)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != len(expected) {
		t.Fatalf("columns=%d want=%d", count, len(expected))
	}
}
