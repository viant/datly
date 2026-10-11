package dml

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestSourceReusesOwnedDialectWithoutChangingTransactions(t *testing.T) {
	type mapping struct {
		Order  int `sqlx:"order_id,primaryKey"`
		Region int `sqlx:"region_id,primaryKey"`
	}
	for _, mode := range []string{"commit", "duplicate rollback", "caller rollback"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE mappings(order_id INTEGER, region_id INTEGER, PRIMARY KEY(order_id,region_id))"); err != nil {
				t.Fatal(err)
			}
			ownedDialect, err := config.Dialect(ctx, h.DB)
			if err != nil {
				t.Fatal(err)
			}
			var metadataCalls atomic.Int32
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if strings.Contains(strings.ToLower(call.SQL), "sqlite_version") {
					metadataCalls.Add(1)
					return errors.New("owned dialect must avoid repeated product discovery")
				}
				return nil
			})
			source := Source{DB: db, Dialect: ownedDialect}
			if mode == "caller rollback" {
				source.Tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer source.Tx.Rollback()
			}
			resolved, strategy, err := source.ResolveSequenceStrategy(ctx)
			if err != nil || strategy != string(ownedDialect.SequenceStrategy("")) {
				t.Fatalf("native default: strategy=%s err=%v", strategy, err)
			}
			value, err := resolved.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data := value.(*Data)
			if data.dialect != ownedDialect {
				t.Fatal("source lost its owner's dialect")
			}
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			rows := []*mapping{{Order: 1, Region: 2}, {Order: 1, Region: 3}}
			if mode == "duplicate rollback" {
				rows[1].Region = 2
			}
			for _, row := range rows {
				if err = data.Insert("mappings", row); err != nil {
					t.Fatal(err)
				}
			}
			err = data.Complete(ctx, nil)
			if mode == "duplicate rollback" {
				if err == nil {
					t.Fatal("duplicate insert succeeded")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if mode == "caller rollback" {
				var count int
				if err = source.Tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); err != nil || count != 2 {
					t.Fatalf("caller rows=%d err=%v", count, err)
				}
				if err = source.Tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "commit" {
				want = 2
			}
			if count != want || metadataCalls.Load() != 0 {
				t.Fatalf("rows=%d want=%d product queries=%d", count, want, metadataCalls.Load())
			}
		})
	}
}

func TestSourceOwnedDialectPreservesExplicitSequenceStrategy(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	ownedDialect, err := config.Dialect(ctx, h.DB)
	if err != nil {
		t.Fatal(err)
	}
	source := Source{DB: h.DB, Dialect: ownedDialect, SequenceStrategy: dialect.PresetIDWithTransientTransaction}
	value, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := value.(*Data)
	if data.sequenceStrategy != dialect.PresetIDWithTransientTransaction || data.dialect != ownedDialect {
		t.Fatal("explicit strategy or owned dialect changed")
	}
}

func TestSourceWithoutOwnedDialectRetainsNativeDiscovery(t *testing.T) {
	h := sqlite.New(t)
	var metadataCalls atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if strings.Contains(strings.ToLower(call.SQL), "sqlite_version") {
			metadataCalls.Add(1)
		}
		return nil
	})
	value, err := (Source{DB: db}).Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value.(*Data).dialect == nil || metadataCalls.Load() == 0 {
		t.Fatal("unconfigured source bypassed native product discovery")
	}
}
