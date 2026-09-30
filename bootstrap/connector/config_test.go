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

func TestConfiguredDriverRequiresNoLiveDatabase(t *testing.T) {
	ctx := context.Background()
	set, err := connector.Open(ctx, []connector.Config{
		{Name: "alias", AliasOf: "main"},
		{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "driver.sqlite")},
	}, "alias")
	if err != nil {
		t.Fatal(err)
	}
	if err = set.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"main", "alias", ""} {
		driver, err := set.ConfiguredDriver(ctx, name)
		if err != nil || driver != "sqlite3" {
			t.Fatalf("configured driver for %q=%q: %v", name, driver, err)
		}
	}
	if _, err = set.ConfiguredDriver(ctx, "missing"); err == nil {
		t.Fatal("unknown driver resolved")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = set.ConfiguredDriver(canceled, "main"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if _, err = (*connector.Set)(nil).ConfiguredDriver(ctx, "main"); err == nil {
		t.Fatal("nil set resolved")
	}
}

func TestConnectorAliasesShareHandleAndTransactionIdentity(t *testing.T) {
	ctx := context.Background()
	set, err := connector.Open(ctx, []connector.Config{
		{Name: "permissions", AliasOf: "authz"},
		{Name: "authz", AliasOf: "studio"},
		{Name: "studio", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "alias.sqlite"), MaxOpenConns: 3},
	}, "permissions")
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	studio, err := set.ResolveDB(ctx, "studio")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"authz", "permissions"} {
		alias, err := set.ResolveDB(ctx, name)
		if err != nil || alias != studio {
			t.Fatalf("%s did not share the managed database identity: %v", name, err)
		}
	}
	if set.SQL.DB != studio || studio.Stats().MaxOpenConnections != 3 {
		t.Fatal("default alias did not retain target pool")
	}
	if _, err := studio.ExecContext(ctx, "CREATE TABLE marker(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	tx, err := studio.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO marker VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	alias, _ := set.ResolveDB(ctx, "authz")
	var count int
	if err = alias.QueryRowContext(ctx, "SELECT COUNT(*) FROM marker").Scan(&count); err != nil || count != 0 {
		t.Fatalf("alias observed rolled-back data: count=%d error=%v", count, err)
	}
	if err = set.Close(); err != nil {
		t.Fatal(err)
	}
	if err = alias.Ping(); err == nil {
		t.Fatal("alias left an owned handle open")
	}
}

func TestConnectorAliasesRejectAmbiguousOrCyclicConfiguration(t *testing.T) {
	for _, configs := range [][]connector.Config{
		{{Name: "alias", AliasOf: "missing"}},
		{{Name: "alias", AliasOf: "alias"}},
		{{Name: "one", AliasOf: "two"}, {Name: "two", AliasOf: "one"}},
		{{Name: "alias", AliasOf: "real", Driver: "sqlite3", DSN: "private-password"}},
		{{Name: "alias", AliasOf: "real", MaxOpenConns: 1}},
		{{Name: "real", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "db")}, {Name: "alias", AliasOf: "real"}, {Name: "alias", AliasOf: "real"}},
	} {
		set, err := connector.Open(context.Background(), configs, "")
		if err == nil || set != nil || strings.Contains(err.Error(), "private-password") {
			t.Fatalf("unsafe alias configuration accepted: set=%v error=%v", set, err)
		}
	}
}
