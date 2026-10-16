package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

func TestConcurrentReaderTransactionResolution(t *testing.T) {
	a, b := sqlite.New(t), sqlite.New(t)
	for n := 0; n < 500; n++ {
		root, _ := invocationDataScope(t.Context(), dml.Source{DB: a.DB})
		if err := root.enrollBufferedScope(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := root.databaseUnit(t.Context(), dml.Source{DB: b.DB}, b.DB, ""); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 4)
		// Multiple readers of each database must join only their own unit's
		// initialization without rejecting another unit that is still opening.
		for _, db := range []*sql.DB{a.DB, b.DB, a.DB, b.DB} {
			go func(db *sql.DB) {
				<-start
				tx, err := root.transactionForDatabase(t.Context(), db)
				if err == nil && tx != nil {
					err = errors.New("transaction lookup started a read-only transaction")
				}
				results <- err
			}(db)
		}
		close(start)
		var failure error
		for i := 0; i < 4; i++ {
			failure = errors.Join(failure, <-results)
		}
		completionErr := root.complete(t.Context(), failure)
		if failure != nil || completionErr != nil {
			t.Fatalf("iteration %d: lookup=%v completion=%v", n, failure, completionErr)
		}
	}
}

type openingOwner struct {
	orderedCustomData
	entered, release chan struct{}
}

func (d *openingOwner) BeginInvocation() error { close(d.entered); <-d.release; return nil }

type openingOwnerSource struct{ data *openingOwner }

func (s openingOwnerSource) Open(context.Context) (xh.Data, error) { return s.data, nil }
func (s openingOwnerSource) InvocationKey() any                    { return s.data }

func TestOrderedAdmissionWaitsForOwnerResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	a := sqlite.New(t)
	root, _ := invocationDataScope(ctx, dml.Source{DB: a.DB})
	if err := root.enrollBufferedScope(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := root.resolve(ctx); err != nil {
		t.Fatal(err)
	}
	custom := &openingOwner{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(custom.release) }) }
	defer release()
	source := openingOwnerSource{data: custom}
	unit, err := root.databaseUnit(ctx, source, source.InvocationKey(), "")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := unit.resolve(ctx); done <- err }()
	select {
	case <-custom.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Open has returned data, but BeginInvocation has not returned. Admission
	// must not read that unfinished ownership state or poison the other reader.
	if _, err := root.transactionForDatabase(ctx, a.DB); err != nil {
		t.Errorf("unresolved owner rejected another reader: %v", err)
	}
	release()
	select {
	case err = <-done:
		if !errors.Is(err, drainowner.ErrOrderedComposition) {
			t.Errorf("resolved custom owner must still be rejected: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = root.complete(ctx, nil); !errors.Is(err, drainowner.ErrOrderedComposition) {
		t.Fatalf("unsupported owner did not veto completion: %v", err)
	}
	if custom.completions != 1 {
		t.Fatalf("custom cleanup calls = %d", custom.completions)
	}
}

func TestConcurrentRequestsKeepDataAndTransactionsIsolated(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE requests(user_id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	connector := &dsql.SQLComponent{DB: h.DB}
	if err := connector.RegisterConnector("main", h.DB); err != nil {
		t.Fatal(err)
	}
	type observation struct {
		id    int
		scope *dataScope
		tx    *sql.Tx
	}
	ready := make(chan observation, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	done := []chan error{make(chan error, 1), make(chan error, 1)}
	abort := errors.New("second request rolled back")
	input := testRouteInput(t, reflect.TypeFor[struct{}]())
	engine := New()
	for id := 0; id < 2; id++ {
		go func(id int) {
			_, err := engine.Execute(ctx, Request{Input: input, DataSource: dml.Source{DB: h.DB}, BufferedComponentCalls: true,
				Capabilities: rh.InvocationCapabilities{Connector: connector},
				Handler: rh.HandlerFunc(func(callCtx context.Context, in rh.Invocation) (any, error) {
					value, found, err := in.Binder.Lookup(callCtx, xh.DataKey)
					if err != nil || !found {
						return nil, fmt.Errorf("data unavailable: %v", err)
					}
					if err = value.(xh.Data).Execute("INSERT INTO requests(user_id) VALUES(?)", id); err != nil {
						return nil, err
					}
					starter, found, err := in.Binder.Lookup(callCtx, xh.TransactionStarterKey)
					if err != nil || !found {
						return nil, fmt.Errorf("starter unavailable: %v", err)
					}
					if err = starter.(xh.TransactionStarter).Start(callCtx); err != nil {
						return nil, err
					}
					connection, err := connector.Resolve(callCtx, "main")
					if err != nil {
						return nil, err
					}
					ready <- observation{id: id, scope: mainScope(callCtx), tx: connection.Tx}
					select {
					case <-release[id]:
					case <-callCtx.Done():
						return nil, callCtx.Err()
					}
					if id == 1 {
						return nil, abort
					}
					return nil, nil
				}),
			})
			done[id] <- err
		}(id)
	}
	var observed [2]observation
	for i := 0; i < 2; i++ {
		select {
		case item := <-ready:
			observed[item.id] = item
		case err := <-done[0]:
			t.Fatalf("first request failed before barrier: %v", err)
		case err := <-done[1]:
			t.Fatalf("second request failed before barrier: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	a, b := observed[0], observed[1]
	if a.scope == b.scope || a.scope.data == b.scope.data || a.scope.nativeInvocation == b.scope.nativeInvocation {
		t.Fatal("independent requests shared a data owner")
	}
	if a.tx == nil || b.tx == nil || a.tx == b.tx {
		t.Fatal("independent requests did not get distinct transactions")
	}
	close(release[0])
	if err := <-done[0]; err != nil {
		t.Fatal(err)
	}
	// Completing one request must not close the other request's transaction.
	var count int
	if err := b.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM requests").Scan(&count); err != nil || count != 1 {
		t.Fatalf("second transaction after first commit: rows=%d err=%v", count, err)
	}
	close(release[1])
	if err := <-done[1]; !errors.Is(err, abort) {
		t.Fatalf("rollback error = %v", err)
	}
	var userID int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*), MIN(user_id) FROM requests").Scan(&count, &userID); err != nil || count != 1 || userID != 0 {
		t.Fatalf("cross-request write leak: rows=%d user=%d err=%v", count, userID, err)
	}
}
