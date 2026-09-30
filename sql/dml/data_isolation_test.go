package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	dexec "github.com/viant/datly/exec"
)

type isolationConnector struct {
	options   []driver.TxOptions
	commits   int
	rollbacks int
	beginErr  error
}

func (c *isolationConnector) Connect(context.Context) (driver.Conn, error) {
	return &isolationConn{owner: c}, nil
}
func (c *isolationConnector) Driver() driver.Driver { return isolationDriver{} }

type isolationDriver struct{}

func (isolationDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("connector required")
}

type isolationConn struct{ owner *isolationConnector }

func (*isolationConn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("unexpected SQL") }
func (*isolationConn) Close() error                        { return nil }
func (*isolationConn) Begin() (driver.Tx, error)           { return nil, fmt.Errorf("BeginTx required") }
func (c *isolationConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.owner.options = append(c.owner.options, options)
	if c.owner.beginErr != nil {
		return nil, c.owner.beginErr
	}
	return isolationTx{owner: c.owner}, nil
}

func TestTransactionIsolationFailureCannotBeCaughtThenCommitted(t *testing.T) {
	for name, query := range map[string]func(*Data, context.Context) error{
		"query": func(d *Data, ctx context.Context) error { _, err := d.QueryContext(ctx, "SELECT 1"); return err },
		"query-row": func(d *Data, ctx context.Context) error {
			var value int
			return d.QueryRowContext(ctx, "SELECT 1").Scan(&value)
		},
		"exec": func(d *Data, ctx context.Context) error { _, err := d.ExecContext(ctx, "SELECT 1"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			connector := &isolationConnector{}
			db := sql.OpenDB(connector)
			defer db.Close()
			data := NewData(db)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err := data.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx := dexec.WithTransactionIsolation(context.Background(), dexec.IsolationSerializable)
			if err := query(data, ctx); err == nil {
				t.Fatal("isolation change accepted")
			}
			if err := data.Complete(context.Background(), nil); err == nil {
				t.Fatal("caught policy failure committed")
			}
			if connector.commits != 0 || connector.rollbacks != 1 {
				t.Fatalf("completion=%+v", connector)
			}
		})
	}
}

func TestManagedTransactionIsolationDriverFailurePropagates(t *testing.T) {
	failure := errors.New("driver rejected requested isolation")
	connector := &isolationConnector{beginErr: failure}
	db := sql.OpenDB(connector)
	defer db.Close()
	data := NewData(db)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	ctx := dexec.WithTransactionIsolation(context.Background(), dexec.IsolationSerializable)
	if err := data.Start(ctx); !errors.Is(err, failure) {
		t.Fatalf("start=%v", err)
	}
	if err := data.Complete(context.Background(), nil); !errors.Is(err, failure) {
		t.Fatalf("complete=%v", err)
	}
	if connector.commits != 0 || connector.rollbacks != 0 {
		t.Fatalf("unexpected completion=%+v", connector)
	}
}

type isolationTx struct{ owner *isolationConnector }

func (t isolationTx) Commit() error   { t.owner.commits++; return nil }
func (t isolationTx) Rollback() error { t.owner.rollbacks++; return nil }

func TestManagedTransactionIsolationReachesDriver(t *testing.T) {
	for _, tc := range []struct {
		policy dexec.TransactionIsolation
		want   sql.IsolationLevel
	}{
		{dexec.IsolationReadCommitted, sql.LevelReadCommitted},
		{dexec.IsolationRepeatableRead, sql.LevelRepeatableRead},
		{dexec.IsolationSerializable, sql.LevelSerializable},
	} {
		t.Run(string(tc.policy), func(t *testing.T) {
			connector := &isolationConnector{}
			db := sql.OpenDB(connector)
			defer db.Close()
			data := NewData(db)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			ctx := dexec.WithTransactionIsolation(context.Background(), tc.policy)
			if err := data.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := data.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := data.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(connector.options) != 1 || connector.options[0].Isolation != driver.IsolationLevel(tc.want) {
				t.Fatalf("driver transaction options: %+v", connector.options)
			}
			if err := data.Complete(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			if connector.commits != 1 || connector.rollbacks != 0 {
				t.Fatalf("completion: %+v", connector)
			}
		})
	}
}

func TestManagedTransactionIsolationCannotChangeExistingUnit(t *testing.T) {
	connector := &isolationConnector{}
	db := sql.OpenDB(connector)
	defer db.Close()
	data := NewData(db)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := dexec.WithTransactionIsolation(context.Background(), dexec.IsolationSerializable)
	if err := data.Start(ctx); err == nil {
		t.Fatal("existing default transaction silently upgraded")
	}
	if len(connector.options) != 1 {
		t.Fatalf("replaced existing transaction: %+v", connector.options)
	}
	if err := data.Complete(context.Background(), nil); err == nil {
		t.Fatal("failed invocation committed")
	}
	if connector.commits != 0 || connector.rollbacks != 1 {
		t.Fatalf("completion: %+v", connector)
	}
}

func TestManagedTransactionIsolationRejectsUnknownAndCallerOwned(t *testing.T) {
	connector := &isolationConnector{}
	db := sql.OpenDB(connector)
	defer db.Close()
	ctx := context.Background()
	data := NewData(db)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Start(dexec.WithTransactionIsolation(ctx, "unknown")); err == nil || len(connector.options) != 0 {
		t.Fatal("unknown isolation reached driver")
	}
	_ = data.Complete(ctx, nil)
	supplied, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	data = NewData(db, WithTx(supplied))
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Start(dexec.WithTransactionIsolation(ctx, dexec.IsolationSerializable)); err == nil {
		t.Fatal("caller transaction isolation was assumed")
	}
	_ = data.Complete(ctx, nil)
	if connector.commits != 0 || connector.rollbacks != 0 {
		t.Fatal("caller transaction completed")
	}
	if err := supplied.Rollback(); err != nil {
		t.Fatal(err)
	}
}
