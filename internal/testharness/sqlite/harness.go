package sqlite

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/testutil/sqlfault"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

type Harness struct {
	DB      *sql.DB
	TempDir string
	dsn     string
}

type Option func(*config)

type config struct {
	dsn string
}

func WithDSN(dsn string) Option {
	return func(c *config) {
		c.dsn = dsn
	}
}

func New(t testing.TB, opts ...Option) *Harness {
	t.Helper()

	cfg := &config{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	tempDir := t.TempDir()
	dsn := cfg.dsn
	if dsn == "" {
		dsn = filepath.Join(tempDir, "test.db")
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return &Harness{
		DB:      db,
		TempDir: tempDir,
		dsn:     dsn,
	}
}

func (h *Harness) FaultDB(t testing.TB, before func(context.Context, sqlfault.Call) error) *sql.DB {
	t.Helper()
	db := sql.OpenDB(&sqlfault.Connector{Base: h.DB.Driver(), DSN: h.dsn, Before: before})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (h *Harness) ExecStatements(ctx context.Context, statements ...string) error {
	for _, stmt := range statements {
		if stmt == "" {
			continue
		}
		if _, err := h.DB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (h *Harness) LoadSQLFile(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return h.ExecStatements(ctx, string(data))
}
