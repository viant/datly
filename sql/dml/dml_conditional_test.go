package dml

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

type casRecord struct {
	Name  string `sqlx:"name,primaryKey"`
	Title string `sqlx:"title"`
	Etag  int    `sqlx:"etag" writer:"concurrency"`
}

type casSparseRecord struct {
	Name  string              `sqlx:"name,primaryKey"`
	Title string              `sqlx:"title"`
	Etag  int                 `sqlx:"etag" writer:"concurrency"`
	Has   *casSparseRecordHas `setMarker:"true" sqlx:"-" json:"-"`
}

type casSparseRecordHas struct {
	Name  bool
	Title bool
	Etag  bool
}

type casTimestampRecord struct {
	Name      string    `sqlx:"name,primaryKey"`
	Title     string    `sqlx:"title"`
	VersionAt time.Time `sqlx:"version_at" writer:"concurrency"`
}

func TestConditionalUpdateChecksPersistedTokenAtExecution(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(name TEXT PRIMARY KEY,title TEXT NOT NULL,etag INTEGER NOT NULL)",
		"INSERT INTO records(name,title,etag) VALUES('one','initial',1)"); err != nil {
		t.Fatal(err)
	}
	first := NewData(h.DB)
	if err := first.UpdateWithOptions("records", &casRecord{Name: "one", Title: "first", Etag: 2}, xhandler.WithIfMatch("etag", 1)); err != nil {
		t.Fatal(err)
	}
	if err := first.Flush(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	stale := NewData(h.DB)
	if err := stale.UpdateWithOptions("records", &casRecord{Name: "one", Title: "stale", Etag: 3}, xhandler.WithIfMatch("etag", 1)); err != nil {
		t.Fatal(err)
	}
	var conflict *xhandler.Conflict
	if err := stale.Flush(ctx, "records"); !errors.As(err, &conflict) {
		t.Fatalf("stale update did not conflict: %v", err)
	}
	queuedBeforeRace := NewData(h.DB)
	if err := queuedBeforeRace.UpdateWithOptions("records", &casRecord{Name: "one", Title: "raced", Etag: 4}, xhandler.WithIfMatch("etag", 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.DB.ExecContext(ctx, "UPDATE records SET title='other',etag=3 WHERE name='one'"); err != nil {
		t.Fatal(err)
	}
	if err := queuedBeforeRace.Flush(ctx, "records"); !errors.As(err, &conflict) {
		t.Fatalf("change between queue and execution did not conflict: %v", err)
	}
	var title string
	var etag int
	if err := h.DB.QueryRowContext(ctx, "SELECT title,etag FROM records WHERE name='one'").Scan(&title, &etag); err != nil || title != "other" || etag != 3 {
		t.Fatalf("conflicting mutation changed row: title=%q etag=%d err=%v", title, etag, err)
	}
}

func TestConditionalUpdatePreservesUnspecifiedFields(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(name TEXT PRIMARY KEY,title TEXT NOT NULL,etag INTEGER NOT NULL)",
		"INSERT INTO records(name,title,etag) VALUES('one','keep',1)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	row := &casSparseRecord{Name: "one", Etag: 2, Has: &casSparseRecordHas{Name: true, Etag: true}}
	if err := data.UpdateWithOptions("records", row, xhandler.WithIfMatch("etag", 1)); err != nil {
		t.Fatal(err)
	}
	if err := data.Flush(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	var title string
	var etag int
	if err := h.DB.QueryRowContext(ctx, "SELECT title,etag FROM records WHERE name='one'").Scan(&title, &etag); err != nil || title != "keep" || etag != 2 {
		t.Fatalf("sparse update title=%q etag=%d err=%v", title, etag, err)
	}
}

func TestConditionalUpdateUsesPersistedTimeToken(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(name TEXT PRIMARY KEY,title TEXT NOT NULL,version_at DATETIME NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	initial := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	if _, err := h.DB.ExecContext(ctx, "INSERT INTO records(name,title,version_at) VALUES(?,?,?)", "one", "initial", initial); err != nil {
		t.Fatal(err)
	}
	var persisted time.Time
	if err := h.DB.QueryRowContext(ctx, "SELECT version_at FROM records WHERE name='one'").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.UpdateWithOptions("records", &casTimestampRecord{Name: "one", Title: "next", VersionAt: persisted.Add(time.Minute)}, xhandler.WithIfMatch("version_at", persisted)); err != nil {
		t.Fatal(err)
	}
	if err := data.Flush(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	var title string
	if err := h.DB.QueryRowContext(ctx, "SELECT title FROM records WHERE name='one'").Scan(&title); err != nil || title != "next" {
		t.Fatalf("time-token update title=%q err=%v", title, err)
	}
}

func TestConditionalDeleteChecksPersistedTokenAtExecution(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(name TEXT PRIMARY KEY,title TEXT NOT NULL,etag INTEGER NOT NULL)",
		"INSERT INTO records(name,title,etag) VALUES('one','initial',1)"); err != nil {
		t.Fatal(err)
	}
	stale := NewData(h.DB)
	if err := stale.DeleteWithOptions("records", &casRecord{Name: "one", Etag: 1}, xhandler.WithIfMatch("etag", 0)); err != nil {
		t.Fatal(err)
	}
	var conflict *xhandler.Conflict
	if err := stale.Flush(ctx, "records"); !errors.As(err, &conflict) {
		t.Fatalf("stale delete did not conflict: %v", err)
	}
	queued := NewData(h.DB)
	if err := queued.DeleteWithOptions("records", &casRecord{Name: "one", Etag: 1}, xhandler.WithIfMatch("etag", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.DB.ExecContext(ctx, "UPDATE records SET etag=2 WHERE name='one'"); err != nil {
		t.Fatal(err)
	}
	if err := queued.Flush(ctx, "records"); !errors.As(err, &conflict) {
		t.Fatalf("change between queue and delete did not conflict: %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE name='one' AND etag=2").Scan(&count); err != nil || count != 1 {
		t.Fatalf("conflicting delete removed row: count=%d err=%v", count, err)
	}
	valid := NewData(h.DB)
	if err := valid.DeleteWithOptions("records", &casRecord{Name: "one", Etag: 2}, xhandler.WithIfMatch("etag", 2)); err != nil {
		t.Fatal(err)
	}
	if err := valid.Flush(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
		t.Fatalf("matching delete left row: count=%d err=%v", count, err)
	}
}
