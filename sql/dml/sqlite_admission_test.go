package dml

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/sqliteadmission"
	xhandler "github.com/viant/xdatly/handler"
)

func admissionDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_txlock=immediate&_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	closeGate, err := sqliteadmission.Register(db, "sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); closeGate() })
	return db
}
func TestSQLiteOwnerAdmissionQueuedCancellationAndNestedReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.db")
	a, b := admissionDB(t, path), admissionDB(t, path)
	ctx := context.Background()
	if _, err := a.Exec("CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	owner := NewData(a)
	if err := owner.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer owner.Complete(ctx, errors.New("cleanup"))
	child := owner.ComponentData(ComponentImperative, "child")
	if err := child.(xhandler.TransactionStarter).Start(ctx); err != nil {
		t.Fatal(err)
	}
	child.(*Data).SealComponent()
	// Readers must not queue behind the SQLite writer admission.
	readCtx, cancelRead := context.WithTimeout(ctx, time.Second)
	defer cancelRead()
	var count int
	if err := b.QueryRowContext(readCtx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	waiting := NewData(b)
	if err := waiting.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := waiting.Start(deadline)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second || waiting.tx != nil {
		t.Fatalf("queued start elapsed=%s tx=%p error=%v", time.Since(start), waiting.tx, err)
	}
	_ = waiting.Complete(ctx, err)
	cause := errors.New("rollback")
	if err := owner.Complete(ctx, cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	next := NewData(b)
	if err := next.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := next.Start(readCtx); err != nil {
		t.Fatal(err)
	}
	if err := next.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
func TestSQLiteAdmissionReleasedBeforeCommitObserver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	a, b := admissionDB(t, path), admissionDB(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var observerErr error
	owner := NewData(a, WithCommitObserver(func(ctx context.Context) {
		next := NewData(b)
		observerErr = next.BeginInvocation()
		if observerErr == nil {
			observerErr = next.Start(ctx)
		}
		if observerErr == nil {
			observerErr = next.Complete(ctx, nil)
		}
	}))
	if err := owner.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := owner.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if observerErr != nil {
		t.Fatalf("commit observer blocked by completed owner: %v", observerErr)
	}
}
