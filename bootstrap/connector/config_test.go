package connector_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestConnectorOriginalPoolSemanticsSQLite(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "db.sqlite")
	db := sqlite.New(t, sqlite.WithDSN(dsn))
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	set, err := connector.Open(context.Background(), []connector.Config{{Name: " main ", Driver: "sqlite3", DSN: dsn, MaxIdleConns: -1, MaxOpenConns: -1, ConnMaxIdleTimeMs: -1, ConnMaxLifetimeMs: -1}}, "main")
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if stats := set.SQL.DB.Stats(); stats.Idle != 0 || stats.MaxOpenConnections != 0 {
		t.Fatalf("pool settings %+v", stats)
	}
	if _, err := set.ResolveDB(context.Background(), "missing"); err == nil {
		t.Fatal("unknown connector resolved")
	}
	if err = set.Close(); err != nil {
		t.Fatal(err)
	}
	if err = set.SQL.DB.Ping(); err == nil {
		t.Fatal("owned connection is open")
	}
	if err = db.DB.Ping(); err != nil {
		t.Fatal("caller connection was closed")
	}
}

func TestConnectorCancellationAndSafeErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connector.Open(ctx, nil, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v", err)
	}
	for _, configs := range [][]connector.Config{
		{{Name: "main", Driver: "not-linked", DSN: "private-password"}},
		{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "missing/db")}},
		{{Name: "main", Driver: "sqlite3", DSN: ":memory:"}, {Name: " main ", Driver: "sqlite3", DSN: ":memory:"}},
	} {
		if set, err := connector.Open(context.Background(), configs, "main"); err == nil || set != nil || strings.Contains(err.Error(), "private-password") {
			t.Fatalf("set=%v err=%v", set, err)
		}
	}
}
