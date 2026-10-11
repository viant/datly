package column

import (
	"context"
	"database/sql"
	"testing"
	"time"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

// Discovery must borrow the invocation transaction even when every pool slot
// is occupied. It must neither acquire a third connection nor complete the tx.
func TestRefinerUsesInvocationTransactionWithOccupiedPool(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	h.DB.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.ExecStatements(ctx, `CREATE TABLE discovery_records (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	occupied, err := h.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	ctx = dexec.WithInvocationTransactionLookup(ctx, func(_ context.Context, db *sql.DB) (*sql.Tx, error) {
		if db != h.DB {
			t.Fatal("discovery selected another database")
		}
		return tx, nil
	})
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "discovery_records"}}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, nil); err != nil {
		t.Fatalf("transactional discovery: %v", err)
	}
	if len(component.RootView.Columns) != 2 || !component.RootView.Columns[0].PrimaryKey {
		t.Fatalf("incomplete discovered metadata: %+v", component.RootView.Columns)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO discovery_records (id, name) VALUES (1, 'pending')`); err != nil {
		t.Fatalf("discovery completed caller transaction: %v", err)
	}
}
