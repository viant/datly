package uow

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type namedOperation string

func (o namedOperation) TableName() string { return string(o) }

type sqlOperation struct {
	table string
	query string
}

func (o sqlOperation) TableName() string { return o.table }

type batchOperation struct{ key, name string }

func (o batchOperation) TableName() string { return o.key }
func (o batchOperation) BatchKey() string  { return o.key }

func TestScopeOrdersParentBeforeBindingAndImperativeAtMarker(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var order []string
	newBuffer := func(frame *Frame) *Buffer {
		return frame.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
			func(_ context.Context, _ *sql.Tx, value any) error {
				order = append(order, string(value.(namedOperation)))
				return nil
			})
	}
	rootBuffer := newBuffer(root)
	if err = rootBuffer.Append(namedOperation("parent-before")); err != nil {
		t.Fatal(err)
	}
	childCtx := PrepareChild(ctx, RelationImperative, "")
	_, _, imperative, _, err := Enter(childCtx, "imperative")
	if err != nil {
		t.Fatal(err)
	}
	if err = newBuffer(imperative).Append(namedOperation("imperative")); err != nil {
		t.Fatal(err)
	}
	imperative.Seal()
	if err = rootBuffer.Append(namedOperation("parent-after")); err != nil {
		t.Fatal(err)
	}
	bindingCtx := PrepareChild(ctx, RelationBinding, "0001")
	_, _, binding, _, err := Enter(bindingCtx, "binding")
	if err != nil {
		t.Fatal(err)
	}
	if err = newBuffer(binding).Append(namedOperation("binding")); err != nil {
		t.Fatal(err)
	}
	binding.Seal()
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"parent-before", "imperative", "parent-after", "binding"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want %v", order, want)
	}
}

func TestScopeRejectsBindingFlushWithOpenParent(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, _, _ := NewRoot(context.Background(), "root")
	childCtx := PrepareChild(ctx, RelationBinding, "0001")
	_, _, child, _, err := Enter(childCtx, "child")
	if err != nil {
		t.Fatal(err)
	}
	buffer := child.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil, func(context.Context, *sql.Tx, any) error { return nil })
	if err = buffer.Append(namedOperation("child")); err != nil {
		t.Fatal(err)
	}
	if err = buffer.Flush(ctx, "child"); !errors.Is(err, ErrBindingFlush) {
		t.Fatalf("err=%v", err)
	}
}

func TestScopeRejectsImperativeDescendantFlushInsideOpenBinding(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, _, _ := NewRoot(context.Background(), "root")
	bindingCtx := PrepareChild(ctx, RelationBinding, "0001")
	bindingCtx, _, _, _, err := Enter(bindingCtx, "binding")
	if err != nil {
		t.Fatal(err)
	}
	imperativeCtx := PrepareChild(bindingCtx, RelationImperative, "")
	_, _, imperative, _, err := Enter(imperativeCtx, "imperative")
	if err != nil {
		t.Fatal(err)
	}
	buffer := imperative.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(context.Context, *sql.Tx, any) error { return nil })
	if err = buffer.Append(namedOperation("child")); err != nil {
		t.Fatal(err)
	}
	if err = buffer.Flush(ctx, "child"); !errors.Is(err, ErrBindingFlush) {
		t.Fatalf("Flush() error=%v", err)
	}
}

func TestScopeCausalFlushIncludesPredecessorsOnce(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var order []string
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			order = append(order, string(value.(namedOperation)))
			return nil
		})
	for _, operation := range []namedOperation{"parent", "raw", "child"} {
		if err := buffer.Append(operation); err != nil {
			t.Fatal(err)
		}
	}
	if err := buffer.Flush(ctx, "child"); err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"parent", "raw", "child"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want %v", order, want)
	}
}

func TestScopeBindingOrderUsesReservedDeclarationOrder(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var order []string
	for _, declaration := range []struct {
		order string
		name  namedOperation
	}{{"00000002", "third"}, {"00000000", "first"}, {"00000001", "second"}} {
		childCtx := PrepareChild(ctx, RelationBinding, declaration.order)
		_, _, child, _, err := Enter(childCtx, string(declaration.name))
		if err != nil {
			t.Fatal(err)
		}
		buffer := child.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
			func(_ context.Context, _ *sql.Tx, value any) error {
				order = append(order, string(value.(namedOperation)))
				return nil
			})
		if err = buffer.Append(declaration.name); err != nil {
			t.Fatal(err)
		}
		child.Seal()
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"first", "second", "third"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want %v", order, want)
	}
}

func TestImperativeFlushStopsBeforeBindingChildren(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var order []string
	newBuffer := func(frame *Frame) *Buffer {
		return frame.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
			func(_ context.Context, _ *sql.Tx, value any) error {
				order = append(order, string(value.(namedOperation)))
				return nil
			})
	}
	bindingCtx := PrepareChild(ctx, RelationBinding, "00000000")
	_, _, binding, _, err := Enter(bindingCtx, "binding")
	if err != nil {
		t.Fatal(err)
	}
	if err = newBuffer(binding).Append(namedOperation("binding")); err != nil {
		t.Fatal(err)
	}
	binding.Seal()
	rootBuffer := newBuffer(root)
	if err = rootBuffer.Append(namedOperation("parent")); err != nil {
		t.Fatal(err)
	}
	imperativeCtx := PrepareChild(ctx, RelationImperative, "")
	_, _, imperative, _, err := Enter(imperativeCtx, "imperative")
	if err != nil {
		t.Fatal(err)
	}
	imperativeBuffer := newBuffer(imperative)
	if err = imperativeBuffer.Append(namedOperation("imperative")); err != nil {
		t.Fatal(err)
	}
	if err = imperativeBuffer.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if want := []string{"parent", "imperative"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("flush order=%v want %v", order, want)
	}
	imperative.Seal()
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"parent", "imperative", "binding"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("final order=%v want %v", order, want)
	}
}

func TestScopeCommitsLocalAndRollsBackOnFailure(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		cause     error
		wantCount int
	}{{"commit", nil, 1}, {"rollback", errors.New("root failed"), 0}} {
		t.Run(testCase.name, func(t *testing.T) {
			db, _ := sql.Open("sqlite3", ":memory:")
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE mutations (id INTEGER PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			ctx, scope, root := NewRoot(context.Background(), "root")
			buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
				func(ctx context.Context, tx *sql.Tx, _ any) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO mutations(id) VALUES (1)`)
					return err
				})
			if err := buffer.Append(namedOperation("mutations")); err != nil {
				t.Fatal(err)
			}
			root.Seal()
			err := scope.Finish(ctx, testCase.cause)
			if testCase.cause == nil && err != nil {
				t.Fatal(err)
			}
			if testCase.cause != nil && !errors.Is(err, testCase.cause) {
				t.Fatalf("err=%v", err)
			}
			var count int
			if err = db.QueryRow(`SELECT COUNT(*) FROM mutations`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != testCase.wantCount {
				t.Fatalf("count=%d want %d", count, testCase.wantCount)
			}
		})
	}
}

func TestScopeLeavesAdoptedTransactionOpen(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE mutations (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, scope, root := NewRoot(context.Background(), "root")
	if err = scope.AdoptTransaction(db, tx); err != nil {
		t.Fatal(err)
	}
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(ctx context.Context, actual *sql.Tx, _ any) error {
			if actual != tx {
				t.Fatal("scope did not use adopted transaction")
			}
			_, execErr := actual.ExecContext(ctx, `INSERT INTO mutations(id) VALUES (1)`)
			return execErr
		})
	if err = buffer.Append(namedOperation("mutations")); err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO mutations(id) VALUES (2)`); err != nil {
		t.Fatalf("adopted transaction was completed: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestScopeCampaignFlightForeignKeyOrderAndRollback(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		cause     error
		wantCount int
	}{{name: "commit", wantCount: 1}, {name: "rollback", cause: errors.New("parent failed")}} {
		t.Run(testCase.name, func(t *testing.T) {
			db, _ := sql.Open("sqlite3", ":memory:")
			defer db.Close()
			if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE campaign (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE campaign_flight (id INTEGER PRIMARY KEY, campaign_id INTEGER REFERENCES campaign(id))"); err != nil {
				t.Fatal(err)
			}
			ctx, scope, root := NewRoot(context.Background(), "campaign")
			newBuffer := func(frame *Frame) *Buffer {
				return frame.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
					func(ctx context.Context, tx *sql.Tx, value any) error {
						_, err := tx.ExecContext(ctx, value.(sqlOperation).query)
						return err
					})
			}
			bindingCtx := PrepareChild(ctx, RelationBinding, "00000000")
			_, _, flight, _, err := Enter(bindingCtx, "flight")
			if err != nil {
				t.Fatal(err)
			}
			if err = newBuffer(flight).Append(sqlOperation{table: "campaign_flight", query: "INSERT INTO campaign_flight(id, campaign_id) VALUES (20, 10)"}); err != nil {
				t.Fatal(err)
			}
			flight.Seal()
			if err = newBuffer(root).Append(sqlOperation{table: "campaign", query: "INSERT INTO campaign(id) VALUES (10)"}); err != nil {
				t.Fatal(err)
			}
			root.Seal()
			err = scope.Finish(ctx, testCase.cause)
			if testCase.cause == nil && err != nil {
				t.Fatal(err)
			}
			if testCase.cause != nil && !errors.Is(err, testCase.cause) {
				t.Fatalf("Finish() error=%v", err)
			}
			for _, table := range []string{"campaign", "campaign_flight"} {
				var count int
				if err = db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != testCase.wantCount {
					t.Fatalf("%s count=%d want=%d err=%v", table, count, testCase.wantCount, err)
				}
			}
		})
	}
}

func TestScopeConcurrentAppendAndRepeatedCompletion(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var mu sync.Mutex
	executed := 0
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(context.Context, *sql.Tx, any) error {
			mu.Lock()
			executed++
			mu.Unlock()
			return nil
		})
	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := buffer.Append(namedOperation("audit")); err != nil {
				t.Errorf("Append() error=%v", err)
			}
		}()
	}
	wait.Wait()
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if executed != 100 {
		t.Fatalf("executed=%d", executed)
	}
	if err := scope.Finish(ctx, nil); !errors.Is(err, ErrCompleted) {
		t.Fatalf("second Finish() error=%v", err)
	}
}

func TestScopeAppendDuringReservedFlushRemainsForCompletion(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			name := string(value.(namedOperation))
			if name == "first" {
				close(started)
				<-release
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		})
	if err := buffer.Append(namedOperation("first")); err != nil {
		t.Fatal(err)
	}
	flushErr := make(chan error, 1)
	go func() { flushErr <- buffer.Flush(ctx, "first") }()
	<-started
	if err := buffer.Append(namedOperation("later")); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"first", "later"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestScopeBatchesOnlyContiguousCompatibleOperations(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var trace []string
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			trace = append(trace, "single:"+value.(batchOperation).name)
			return nil
		})
	buffer.SetBatchExecutor(func(_ context.Context, _ *sql.Tx, values []any) error {
		names := "batch"
		for _, value := range values {
			names += ":" + value.(batchOperation).name
		}
		trace = append(trace, names)
		return nil
	})
	for _, operation := range []batchOperation{{"insert:a", "1"}, {"insert:a", "2"}, {"update:a", "3"}, {"insert:a", "4"}} {
		if err := buffer.Append(operation); err != nil {
			t.Fatal(err)
		}
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"batch:1:2", "single:3", "single:4"}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("trace=%v want=%v", trace, want)
	}
}

func TestBufferReconcilePreservesImperativeMarkers(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	var order []string
	newBuffer := func(frame *Frame) *Buffer {
		return frame.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
			func(_ context.Context, _ *sql.Tx, value any) error {
				order = append(order, string(value.(namedOperation)))
				return nil
			})
	}
	rootBuffer := newBuffer(root)
	_ = rootBuffer.Append(namedOperation("old-1"))
	imperativeCtx := PrepareChild(ctx, RelationImperative, "")
	_, _, imperative, _, err := Enter(imperativeCtx, "imperative")
	if err != nil {
		t.Fatal(err)
	}
	_ = newBuffer(imperative).Append(namedOperation("child"))
	imperative.Seal()
	_ = rootBuffer.Append(namedOperation("old-2"))
	if err = rootBuffer.Reconcile([]any{namedOperation("new-1"), namedOperation("new-2")}); err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"new-1", "child", "new-2"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestEnterStartsFreshRootButRejectsCapturedCompletedChild(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	childCtx := PrepareChild(ctx, RelationImperative, "")
	childCtx, _, child, _, err := Enter(childCtx, "child")
	if err != nil {
		t.Fatal(err)
	}
	child.Seal()
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err = Enter(childCtx, "stale-child"); !errors.Is(err, ErrCompleted) {
		t.Fatalf("captured child Enter() error=%v", err)
	}
	_, fresh, _, created, err := Enter(ctx, "fresh-root")
	if err != nil {
		t.Fatal(err)
	}
	if !created || fresh == scope {
		t.Fatalf("created=%v fresh scope reused=%v", created, fresh == scope)
	}
}

func TestSealedFrameRejectsReuseAndAppendBeforeRootCompletion(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	childCtx := PrepareChild(ctx, RelationImperative, "")
	childCtx, _, child, _, err := Enter(childCtx, "child")
	if err != nil {
		t.Fatal(err)
	}
	buffer := child.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(context.Context, *sql.Tx, any) error { return nil })
	child.Seal()
	if err = buffer.Append(namedOperation("late")); !errors.Is(err, ErrFrameSealed) {
		t.Fatalf("Append() error=%v", err)
	}
	if _, _, _, _, err = Enter(childCtx, "reused"); !errors.Is(err, ErrFrameSealed) {
		t.Fatalf("Enter() error=%v", err)
	}
	root.Seal()
	if err = scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestFailureIsTerminalForAppendFlushAndFinish(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	wantErr := errors.New("write failed")
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(context.Context, *sql.Tx, any) error { return wantErr })
	if err := buffer.Append(namedOperation("first")); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(ctx, "first"); !errors.Is(err, wantErr) {
		t.Fatalf("Flush() error=%v", err)
	}
	if err := buffer.Append(namedOperation("second")); !errors.Is(err, ErrFailed) || !errors.Is(err, wantErr) {
		t.Fatalf("Append() error=%v", err)
	}
	if err := buffer.Flush(ctx, "first"); !errors.Is(err, ErrFailed) || !errors.Is(err, wantErr) {
		t.Fatalf("second Flush() error=%v", err)
	}
	if err := scope.Finish(ctx, nil); !errors.Is(err, ErrFailed) || !errors.Is(err, wantErr) {
		t.Fatalf("Finish() error=%v", err)
	}
}

func TestFlushAndFinishAreSerialized(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := NewRoot(context.Background(), "root")
	started := make(chan struct{})
	release := make(chan struct{})
	buffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(context.Context, *sql.Tx, any) error {
			close(started)
			<-release
			return nil
		})
	if err := buffer.Append(namedOperation("first")); err != nil {
		t.Fatal(err)
	}
	flushErr := make(chan error, 1)
	finishErr := make(chan error, 1)
	go func() { flushErr <- buffer.Flush(ctx, "first") }()
	<-started
	go func() { finishErr <- scope.Finish(ctx, nil) }()
	close(release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}
	if err := <-finishErr; err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(ctx, "first"); !errors.Is(err, ErrCompleted) {
		t.Fatalf("post-completion Flush() error=%v", err)
	}
}

func TestReserveBindingOrderIsUniqueAcrossConcurrentResolvers(t *testing.T) {
	ctx, _, _ := NewRoot(context.Background(), "root")
	const count = 100
	orders := make(chan string, count)
	errs := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			order, err := ReserveBindingOrder(ctx)
			orders <- order
			errs <- err
		}()
	}
	wait.Wait()
	close(orders)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	unique := map[string]bool{}
	for order := range orders {
		if unique[order] {
			t.Fatalf("duplicate binding order %q", order)
		}
		unique[order] = true
	}
	if len(unique) != count {
		t.Fatalf("orders=%d want=%d", len(unique), count)
	}
}

func TestPropagateRetainsDestinationCancellationAndInvocationFrame(t *testing.T) {
	source, scope, frame := NewRoot(context.Background(), "root")
	destination, cancel := context.WithCancel(context.Background())
	cancel()
	propagated := Propagate(source, destination)
	gotScope, gotFrame, ok := FromContext(propagated)
	if !ok || gotScope != scope || gotFrame != frame {
		t.Fatalf("scope propagated=%v scopeMatch=%v frameMatch=%v", ok, gotScope == scope, gotFrame == frame)
	}
	if !errors.Is(propagated.Err(), context.Canceled) {
		t.Fatalf("Err()=%v", propagated.Err())
	}
}
