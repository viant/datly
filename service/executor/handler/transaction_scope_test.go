package handler

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	xsqlx "github.com/viant/xdatly/handler/sqlx"
)

func TestHandlerSessionProvidesRootOwnedTransaction(t *testing.T) {
	connector := view.NewConnector("main", "sqlite3", t.TempDir()+"/provider.db")
	db, err := connector.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	aView := &view.View{Connector: connector}
	aView.SetResource(&view.Resource{})
	aSession := session.New(aView)
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	executor := NewExecutor(aView, aSession)
	handlerSession, err := executor.NewHandlerSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	provider, ok := handlerSession.(interface {
		Transaction(context.Context) (*sql.Tx, error)
	})
	if !ok {
		t.Fatal("handler session does not expose transaction capability")
	}
	tx, err := provider.Transaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil {
		t.Fatal("expected invocation transaction")
	}
	service, err := executor.newSqlService(&xsqlx.Options{})
	if err != nil {
		t.Fatal(err)
	}
	bufferTx, err := service.(*Service).buffer.Transaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bufferTx != tx {
		t.Fatal("session capability and mutation buffer use different transactions")
	}
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

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

func TestScopedExecutorRetainsInvocationContextForLegacyUnscopedCall(t *testing.T) {
	aView := &view.View{Name: "patch"}
	aSession := session.New(aView)
	ctx, _, frame := uow.NewRoot(context.Background(), "PATCH /component")
	executor := NewExecutor(aView, aSession)
	if err := executor.ensureUnitOfWork(ctx); err != nil {
		t.Fatal(err)
	}

	if err := executor.ensureUnitOfWork(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, captured, ok := uow.FromContext(executor.ctx)
	if !ok {
		t.Fatal("scoped executor lost its invocation context")
	}
	if captured != frame {
		t.Fatalf("captured frame=%s, want %s", captured.DebugLabel(), frame.DebugLabel())
	}
}

func TestHandlerHttpFacadeRestoresChildInvocationContext(t *testing.T) {
	parentCtx, _, _ := uow.NewRoot(context.Background(), "PATCH /parent")
	childCtx := uow.PrepareChild(parentCtx, uow.RelationImperative, "")
	childCtx, _, childFrame, _, err := uow.Enter(childCtx, "GET /child")
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{ctx: childCtx}
	httpFacade := &Httper{executor: executor}

	restored := httpFacade.invocationContext(parentCtx)
	_, restoredFrame, ok := uow.FromContext(restored)
	if !ok {
		t.Fatal("HTTP facade lost the child invocation context")
	}
	if restoredFrame != childFrame {
		t.Fatalf("restored frame=%s, want %s", restoredFrame.DebugLabel(), childFrame.DebugLabel())
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
