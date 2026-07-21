package handler

import (
	"context"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	xsqlx "github.com/viant/xdatly/handler/sqlx"
)

func TestScopedSQLServiceRejectsConflictingTransaction(t *testing.T) {
	connector := view.NewConnector("main", "sqlite3", t.TempDir()+"/conflict.db")
	db, err := connector.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	txA, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback()
	txB, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txB.Rollback()
	aView := &view.View{Connector: connector}
	aSession := session.New(aView, session.WithSQLTx(txA))
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	if err = scope.AdoptTransaction(db, txA); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(aView, aSession)
	if err = executor.ensureUnitOfWork(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = executor.newSqlService(&xsqlx.Options{WithTx: txB}); !errors.Is(err, uow.ErrTransactionConflict) {
		t.Fatalf("newSqlService() error=%v", err)
	}
	root.Seal()
	if err = scope.Finish(ctx, errors.New("abort")); err == nil {
		t.Fatal("expected root abort")
	}
}

func TestScopedSQLServiceDoesNotExposeRootTransaction(t *testing.T) {
	connector := view.NewConnector("main", "sqlite3", t.TempDir()+"/access.db")
	db, err := connector.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	aView := &view.View{Connector: connector}
	aSession := session.New(aView)
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	executor := NewExecutor(aView, aSession)
	if err = executor.ensureUnitOfWork(ctx); err != nil {
		t.Fatal(err)
	}
	service, err := executor.newSqlService(&xsqlx.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Tx(ctx); !errors.Is(err, uow.ErrTransactionAccess) {
		t.Fatalf("Tx() error=%v", err)
	}
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestScopedSQLServiceDoesNotExposeSuppliedTransaction(t *testing.T) {
	connector := view.NewConnector("main", "sqlite3", t.TempDir()+"/external-access.db")
	db, err := connector.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	aView := &view.View{Connector: connector}
	aSession := session.New(aView, session.WithSQLTx(tx))
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	executor := NewExecutor(aView, aSession)
	if err = executor.ensureUnitOfWork(ctx); err != nil {
		t.Fatal(err)
	}
	service, err := executor.newSqlService(&xsqlx.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Tx(ctx); !errors.Is(err, uow.ErrTransactionAccess) {
		t.Fatalf("Tx() error=%v", err)
	}
	root.Seal()
	if err = scope.Finish(ctx, errors.New("abort")); err == nil {
		t.Fatal("expected root abort")
	}
}
