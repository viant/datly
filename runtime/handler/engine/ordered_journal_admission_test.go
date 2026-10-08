package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// Compatibility counterparts to the original buffered caller-owned variants
// in NativeAdmission49TwoUnitCleanup and NativeCaller49PendingNeverInvokesOwned-
// CommitObserver. Their unbuffered guard controls remain in those existing tests.
func TestOrderedJournalRejectsCallerOwnedCombinations(t *testing.T) {
	for _, rootCaller := range []bool{false, true} {
		for _, childCaller := range []bool{false, true} {
			if !rootCaller && !childCaller {
				continue
			}
			t.Run(fmt.Sprintf("rootCaller=%t/childCaller=%t", rootCaller, childCaller), func(t *testing.T) {
				a, b := sqlite.New(t), sqlite.New(t)
				for _, h := range []*sqlite.Harness{a, b} {
					if err := h.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER PRIMARY KEY)"); err != nil {
						t.Fatal(err)
					}
				}
				var ta, tb *sql.Tx
				var err error
				if rootCaller {
					ta, err = a.DB.BeginTx(t.Context(), nil)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { ta.Rollback() })
					if _, err = ta.Exec("INSERT INTO records VALUES(99)"); err != nil {
						t.Fatal(err)
					}
				}
				if childCaller {
					tb, err = b.DB.BeginTx(t.Context(), nil)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { tb.Rollback() })
					if _, err = tb.Exec("INSERT INTO records VALUES(99)"); err != nil {
						t.Fatal(err)
					}
				}
				var root *dataScope
				published := false
				var caught error
				result, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: dml.Source{DB: a.DB, Tx: ta}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
					root = mainScope(ctx)
					value, _, err := in.Binder.Lookup(ctx, xh.DataKey)
					if err != nil {
						return nil, err
					}
					data := value.(xh.Data)
					if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
						return nil, err
					}
					if err = root.data.(xh.TransactionStarter).Start(ctx); err != nil {
						return nil, err
					}
					_, caught = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: b.DB, Tx: tb}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
						_, found, err := in.Binder.Lookup(ctx, xh.DataKey)
						published = found && err == nil
						return nil, err
					})})
					return "caught rejection", nil
				})})
				if result != nil || !errors.Is(err, drainowner.ErrOrderedComposition) || !errors.Is(caught, drainowner.ErrOrderedComposition) || published {
					t.Fatalf("result=%v err=%v caught=%v published=%t", result, err, caught, published)
				}
				if !rootCaller {
					if state := root.data.(*dml.Data).TransactionOutcome().State; state != xh.TransactionRolledBack {
						t.Fatal(state)
					}
					var n int
					if e := a.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&n); e != nil || n != 0 {
						t.Fatalf("local rollback=%d err=%v", n, e)
					}
				}
				for _, tx := range []*sql.Tx{ta, tb} {
					if tx == nil {
						continue
					}
					var n int
					if e := tx.QueryRow("SELECT COUNT(*) FROM records").Scan(&n); e != nil || n != 1 {
						t.Fatalf("caller preexisting rows=%d err=%v", n, e)
					}
					if _, e := tx.Exec("INSERT INTO records VALUES(100)"); e != nil {
						t.Fatalf("caller ownership lost: %v", e)
					}
				}
			})
		}
	}
}

func TestOrderedJournalRejectsPreviouslyDrainedOwner(t *testing.T) {
	a, b := sqlite.New(t), sqlite.New(t)
	for _, h := range []*sqlite.Harness{a, b} {
		if err := h.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
	}
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a.DB})
	data, err := root.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = data.Flush(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	if err = root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), dml.Source{DB: b.DB})
	if _, err = child.resolve(t.Context()); !errors.Is(err, drainowner.ErrOrderedComposition) {
		t.Fatalf("admission=%v", err)
	}
	child.seal()
	if err = root.complete(t.Context(), nil); !errors.Is(err, drainowner.ErrOrderedComposition) {
		t.Fatalf("caught rejection=%v", err)
	}
	for _, h := range []*sqlite.Harness{a, b} {
		var n int
		if err = h.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&n); err != nil || n != 0 {
			t.Fatalf("durable=%d err=%v", n, err)
		}
	}
}

func TestOrderedJournalFailedBeginHasNoOrdinal(t *testing.T) {
	a, b := sqlite.New(t), sqlite.New(t)
	sentinel := errors.New("injected begin failure")
	bad := orderedBeginDB(t, b, func(context.Context) error { return sentinel })
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a.DB})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad, err := root.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), dml.Source{DB: bad})
	bd, err := child.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = ad.(xh.TransactionStarter).Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = bd.(xh.TransactionStarter).Start(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("begin=%v", err)
	}
	child.seal()
	owners := root.nativeInvocation.TransactionOwners()
	if len(owners) != 1 || owners[0] != root.data {
		t.Fatalf("successful transaction ledger=%v", owners)
	}
	if err = root.complete(t.Context(), sentinel); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if state := child.unit.data.(*dml.Data).TransactionOutcome().State; state != xh.TransactionNone {
		t.Fatal(state)
	}
	if state := root.data.(*dml.Data).TransactionOutcome().State; state != xh.TransactionRolledBack {
		t.Fatal(state)
	}
}

func TestOrderedJournalConcurrentStartupSerialization(t *testing.T) {
	a, b := sqlite.New(t), sqlite.New(t)
	enteredB, releaseB, enteredA := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	dbB := orderedBeginDB(t, b, func(context.Context) error { close(enteredB); <-releaseB; return nil })
	dbA := orderedBeginDB(t, a, func(context.Context) error { enteredA <- struct{}{}; return nil })
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: dbA})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad, err := root.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), dml.Source{DB: dbB})
	bd, err := child.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	doneB, doneA := make(chan error, 1), make(chan error, 1)
	go func() { doneB <- bd.(xh.TransactionStarter).Start(t.Context()) }()
	<-enteredB
	attemptedA := make(chan struct{})
	go func() { close(attemptedA); doneA <- ad.(xh.TransactionStarter).Start(t.Context()) }()
	<-attemptedA
	select {
	case <-enteredA:
		t.Fatal("A entered Begin while B held establishment gate")
	default:
	}
	close(releaseB)
	if err = <-doneB; err != nil {
		t.Fatal(err)
	}
	if err = <-doneA; err != nil {
		t.Fatal(err)
	}
	child.seal()
	owners := root.nativeInvocation.TransactionOwners()
	if len(owners) != 2 || owners[0] != child.unit.data || owners[1] != root.data {
		t.Fatalf("successful creation order=%v", owners)
	}
	if err = root.complete(t.Context(), errors.New("precommit abort")); err == nil {
		t.Fatal("abort lost")
	}
}

// This wrapper changes only Begin admission; all transactions and rollback
// effects remain the actual SQLite driver, with its dialect identity preserved.
type orderedBeginConnector struct {
	base   driver.Driver
	dsn    string
	before func(context.Context) error
}

func (c *orderedBeginConnector) Driver() driver.Driver { return c.base }
func (c *orderedBeginConnector) Connect(ctx context.Context) (driver.Conn, error) {
	raw, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &orderedBeginConn{Conn: raw, before: c.before}, nil
}

type orderedBeginConn struct {
	driver.Conn
	before func(context.Context) error
}

func (c *orderedBeginConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := c.before(ctx); err != nil {
		return nil, err
	}
	if raw, ok := c.Conn.(driver.ConnBeginTx); ok {
		return raw.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}
func (c *orderedBeginConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func orderedBeginDB(t *testing.T, h *sqlite.Harness, before func(context.Context) error) *sql.DB {
	t.Helper()
	db := sql.OpenDB(&orderedBeginConnector{base: h.DB.Driver(), dsn: filepath.Join(h.TempDir, "test.db"), before: before})
	t.Cleanup(func() { db.Close() })
	return db
}

type orderedCustomData struct {
	preBindingData
	completions int
	cause       error
}

func (*orderedCustomData) BeginInvocation() error { return nil }
func (d *orderedCustomData) Complete(_ context.Context, cause error) error {
	d.completions++
	d.cause = cause
	return cause
}
func (*orderedCustomData) CloseMutationAdmission() error { return nil }

type orderedCustomSource struct{ data *orderedCustomData }

func (s orderedCustomSource) Open(context.Context) (xh.Data, error) { return s.data, nil }
func (s orderedCustomSource) InvocationKey() any                    { return s.data }
func TestOrderedJournalRejectsCustomOwnerBeforeCapabilityPublication(t *testing.T) {
	a := sqlite.New(t)
	if err := a.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a.DB})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad, err := root.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = ad.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = ad.(xh.TransactionStarter).Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	custom := &orderedCustomData{}
	child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), orderedCustomSource{data: custom})
	if capability, err := child.resolve(t.Context()); capability != nil || !errors.Is(err, drainowner.ErrOrderedComposition) {
		t.Fatalf("published=%v err=%v", capability, err)
	}
	child.seal()
	if err = root.complete(t.Context(), nil); !errors.Is(err, drainowner.ErrOrderedComposition) {
		t.Fatalf("caught custom error=%v", err)
	}
	if root.data.(*dml.Data).TransactionOutcome().State != xh.TransactionRolledBack {
		t.Fatal("local transaction not rolled back")
	}
	if custom.completions != 1 || !errors.Is(custom.cause, drainowner.ErrOrderedComposition) {
		t.Fatalf("normal custom cleanup=%d cause=%v", custom.completions, custom.cause)
	}
}

func TestOrderedJournalMultipleOwnersRemainRecoveryIneligible(t *testing.T) {
	a, b := sqlite.New(t), sqlite.New(t)
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a.DB})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	if _, err := root.resolve(t.Context()); err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), dml.Source{DB: b.DB})
	if _, err := child.resolve(t.Context()); err != nil {
		t.Fatal(err)
	}
	child.seal()
	cause := errors.New("injected precommit failure")
	if err := root.complete(t.Context(), cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	calls := 0
	probe := &recoveryProbe{recover: func(context.Context, rh.Invocation, any, rh.MutationOutcome) (rh.Recovery, error) {
		calls++
		return rh.RecoveryRetry, nil
	}}
	root.finalizers = []*outcomeFrame{{finished: true}}
	decision, recovered, err := recoverMutation(t.Context(), Request{Handler: probe}, root, rh.Invocation{}, cause)
	if decision != rh.RecoveryNone || recovered || err != nil || calls != 0 || len(root.units) != 1 {
		t.Fatalf("decision=%v recovered=%t err=%v replayCalls=%d owners=%d", decision, recovered, err, calls, len(root.units)+1)
	}
}
