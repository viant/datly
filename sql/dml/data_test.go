package dml

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness"
	sqldialect "github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestDataCachesDMLServicesPerTable(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	err := h.ExecStatements(context.Background(),
		`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);`,
		`DELETE FROM users;`,
	)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	data := NewData(h.DB)
	if err := data.Insert("users", &struct {
		Name string `sqlx:"name"`
	}{Name: "john"}); err != nil {
		t.Fatalf("insert buffer failed: %v", err)
	}
	if err := data.Flush(context.Background(), "users"); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if got := len(data.insertServices); got != 1 {
		t.Fatalf("expected 1 cached insert service, got %d", got)
	}

	if err := data.Insert("users", &struct {
		Name string `sqlx:"name"`
	}{Name: "jane"}); err != nil {
		t.Fatalf("second insert buffer failed: %v", err)
	}
	if err := data.Flush(context.Background(), "users"); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}
	if got := len(data.insertServices); got != 1 {
		t.Fatalf("expected insert service cache reuse, got %d entries", got)
	}
}

func TestDataFlushInsertsRecordsThatNeedBackfillWithoutBatching(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	err := h.ExecStatements(context.Background(),
		`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);`,
		`DELETE FROM users;`,
	)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	data := NewData(h.DB)
	type user struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	u1 := &user{Name: "john"}
	u2 := &user{Name: "jane"}
	if err := data.Insert("users", u1); err != nil {
		t.Fatalf("first insert buffer failed: %v", err)
	}
	if err := data.Insert("users", u2); err != nil {
		t.Fatalf("second insert buffer failed: %v", err)
	}
	if err := data.Flush(context.Background(), "users"); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestDataFlushExecutesCompatibleInsertBatchWithPresetIDs(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	err := h.ExecStatements(context.Background(),
		`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);`,
		`DELETE FROM users;`,
	)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	data := NewData(h.DB)
	dialect, err := data.dialectFor(context.Background(), h.DB)
	if err != nil {
		t.Fatalf("dialect resolution failed: %v", err)
	}
	forcedDialect := *dialect
	forcedDialect.Insert = sqldialect.InsertWithMultiValues
	data.dialect = &forcedDialect
	type user struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	u1 := &user{ID: 101, Name: "john"}
	u2 := &user{ID: 102, Name: "jane"}
	if !canBatchInsert([]*dataOperation{
		{kind: dataOpInsert, table: "users", data: u1},
		{kind: dataOpInsert, table: "users", data: u2},
	}) {
		t.Fatalf("expected preset-id records to be batchable")
	}
	if err := data.Insert("users", u1); err != nil {
		t.Fatalf("first insert buffer failed: %v", err)
	}
	if err := data.Insert("users", u2); err != nil {
		t.Fatalf("second insert buffer failed: %v", err)
	}
	if err := data.Flush(context.Background(), "users"); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if u1.ID != 101 || u2.ID != 102 {
		t.Fatalf("expected preset ids to remain stable, got u1=%+v u2=%+v", u1, u2)
	}
	var count int
	if err := h.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestDataFlushExecutesCompatibleInsertBatchAcrossChunks(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	err := h.ExecStatements(context.Background(),
		`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);`,
		`DELETE FROM users;`,
	)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	data := NewData(h.DB)
	dialect, err := data.dialectFor(context.Background(), h.DB)
	if err != nil {
		t.Fatalf("dialect resolution failed: %v", err)
	}
	forcedDialect := *dialect
	forcedDialect.Insert = sqldialect.InsertWithMultiValues
	data.dialect = &forcedDialect
	type user struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	users := make([]*user, 0, defaultInsertBatchSize+1)
	for i := 0; i < defaultInsertBatchSize+1; i++ {
		record := &user{ID: i + 1, Name: "user"}
		users = append(users, record)
		if err := data.Insert("users", record); err != nil {
			t.Fatalf("insert buffer failed at %d: %v", i, err)
		}
	}
	if err := data.Flush(context.Background(), "users"); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != len(users) {
		t.Fatalf("expected %d inserted rows, got %d", len(users), count)
	}
}

func TestDataFlushRetainsBufferedOperationsAfterFailure(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.Execute(`INSERT INTO audit(id) VALUES (1)`); err != nil {
		t.Fatalf("buffer valid statement: %v", err)
	}
	if err := data.Execute(`INSERT INTO missing_table(id) VALUES (1)`); err != nil {
		t.Fatalf("buffer failed: %v", err)
	}
	if err := data.Flush(ctx, ""); err == nil {
		t.Fatal("expected flush failure")
	}
	if len(data.queue) != 2 {
		t.Fatalf("expected failed operation to remain buffered, got %d", len(data.queue))
	}
	if data.tx != nil {
		t.Fatal("expected failed owned transaction to be rolled back and cleared")
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("local flush must roll back partial execution: count=%d err=%v", count, err)
	}
}

func TestDataAllocateUsesNativeReservation(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	err := h.ExecStatements(context.Background(),
		`CREATE TABLE IF NOT EXISTS users (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT);`,
		`DELETE FROM users;`,
	)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type user struct {
		ID int64 `sqlx:"ID,primaryKey=true"`
	}
	data := NewData(h.DB)
	first := &user{}
	if err := data.Allocate(context.Background(), "users", first, "ID"); err != nil {
		t.Fatalf("allocate failed: %v", err)
	}
	if first.ID == 0 {
		t.Fatalf("expected allocated id")
	}
	if data.tx != nil {
		t.Fatal("sequence allocation must not open the business transaction")
	}
}

func TestSourceFlushLeavesParentTransactionToItsOwner(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := (Source{DB: h.DB, Tx: tx}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = data.Execute(`INSERT INTO audit(id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err = data.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("parent transaction row count = %d, err = %v", count, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("parent rollback row count = %d, err = %v", count, err)
	}
}
