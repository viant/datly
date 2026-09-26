package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestSQLComponent_ResolveNamedConnector(t *testing.T) {
	ctx := context.Background()
	defaultDB := testharness.NewSQLiteHarness(t)
	namedDB := testharness.NewSQLiteHarness(t)
	component := &SQLComponent{DB: defaultDB.DB}

	connection, err := component.Resolve(ctx, "single-name")
	if err != nil {
		t.Fatalf("resolve sole source: %v", err)
	}
	if connection.DB != defaultDB.DB || connection.Dialect == nil {
		t.Fatalf("unexpected sole-source resolution: %+v", connection)
	}
	if err := component.RegisterConnector("analytics", namedDB.DB); err != nil {
		t.Fatalf("register connector: %v", err)
	}
	connection, err = component.Resolve(ctx, "analytics")
	if err != nil {
		t.Fatalf("resolve named connector: %v", err)
	}
	if connection.DB != namedDB.DB || connection.Dialect == nil {
		t.Fatalf("unexpected named resolution: %+v", connection)
	}
	if _, err := component.Resolve(ctx, "missing"); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unknown connector error, got %v", err)
	}
}

func TestSQLComponent_RegisterConnectorValidatesInput(t *testing.T) {
	component := &SQLComponent{}
	if err := component.RegisterConnector("", nil); err == nil {
		t.Fatal("expected empty connector name error")
	}
	if err := component.RegisterConnector("analytics", nil); err == nil {
		t.Fatal("expected nil connector db error")
	}
}

func TestSQLComponent_DialectRequiresDefaultDB(t *testing.T) {
	component := &SQLComponent{}
	if _, err := component.Dialect(context.Background()); err == nil {
		t.Fatal("expected nil db error")
	}
}

func TestSQLComponent_TransactionRejectsDifferentNamedDatabase(t *testing.T) {
	ctx := context.Background()
	defaultDB := testharness.NewSQLiteHarness(t)
	namedDB := testharness.NewSQLiteHarness(t)
	tx, err := defaultDB.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	component := &SQLComponent{DB: defaultDB.DB, Tx: tx}
	if err := component.RegisterConnector("studio", defaultDB.DB); err != nil {
		t.Fatal(err)
	}
	if err := component.RegisterConnector("outside", namedDB.DB); err != nil {
		t.Fatal(err)
	}
	connection, err := component.Resolve(ctx, "studio")
	if err != nil || connection.Tx != tx {
		t.Fatalf("same-DB transaction connection=%+v err=%v", connection, err)
	}
	if _, err := component.Resolve(ctx, "outside"); err == nil {
		t.Fatal("transaction escaped to different named database")
	}
}
