package engine

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"sync"
	"testing"
)

func enrollmentFixture(t *testing.T) (context.Context, *sqlite.Harness, *sqlite.Harness, *dataScope, *dataScope) {
	t.Helper()
	ctx := context.Background()
	first, second := sqlite.New(t), sqlite.New(t)
	for _, db := range []*sql.DB{first.DB, second.DB} {
		for _, q := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"} {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	root, _ := invocationDataScope(ctx, dml.Source{DB: first.DB})
	data, err := root.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: second.DB})
	return ctx, first, second, root, child
}
func enrollmentAssertRolledBack(t *testing.T, dbs ...*sql.DB) {
	t.Helper()
	for _, db := range dbs {
		var rows, audits int
		if err := db.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audits); err != nil {
			t.Fatal(err)
		}
		if rows != 0 || audits != 0 {
			t.Fatalf("caught opt-in error committed rows=%d audit=%d", rows, audits)
		}
	}
}

func TestCapturedEnrollmentPriorUseInDifferentUnitIsSticky(t *testing.T) {
	for _, api := range []string{"rows", "row"} {
		t.Run(api, func(t *testing.T) {
			ctx, first, second, root, child := enrollmentFixture(t)
			data, err := child.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			service := data.(rhandler.TransactionSQL)
			proxy := transactionSQLCapability{service: service, guard: root.mutationGuard()}
			if api == "rows" {
				rows, e := proxy.QueryContext(context.Background(), "SELECT 1")
				if e != nil {
					t.Fatal(e)
				}
				rows.Close()
			} else {
				var id int
				if e := proxy.QueryRowContext(context.Background(), "SELECT 1").Scan(&id); e != nil {
					t.Fatal(e)
				}
			}
			calls := 0
			if err = root.registerExecutionGuard(ctx, func(context.Context) error { calls++; return nil }); err == nil {
				t.Fatal("prior other-unit streaming admitted opt-in")
			}
			// Deliberately swallow the enrollment error.
			if err = root.complete(ctx, nil); err == nil {
				t.Fatal("caught opt-in error allowed owned commit")
			}
			if calls != 0 {
				t.Fatal("rejected guard executed")
			}
			enrollmentAssertRolledBack(t, first.DB, second.DB)
		})
	}
}

func TestCapturedEnrollmentNewUnitBeforeCapabilityPublication(t *testing.T) {
	for _, mode := range []string{"native", "proxy"} {
		t.Run(mode, func(t *testing.T) {
			ctx, first, second, root, child := enrollmentFixture(t)
			if err := root.registerExecutionGuard(ctx, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			data, err := child.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			native := child.unit.data.(*dml.Data)
			if mode == "native" {
				var id int
				err = native.QueryRowContext(ctx, "INSERT INTO nonexistent VALUES(3) RETURNING id").Scan(&id)
			} else {
				proxy := transactionSQLCapability{service: native, guard: child.mutationGuard()}
				rows, e := proxy.QueryContext(context.Background(), "INSERT INTO nonexistent VALUES(3) RETURNING id")
				err = e
				if rows != nil {
					rows.Close()
					t.Fatal("rejected proxy returned rows")
				}
			}
			if err == nil {
				t.Fatal("new unit exposed streaming capability")
			}
			if mode == "native" && !errors.Is(err, dml.ErrGuardedStreamingQuery) {
				t.Fatalf("driver reached: %v", err)
			}
			if err = root.complete(ctx, nil); err == nil {
				t.Fatal("caught new-unit streaming rejection allowed commit")
			}
			enrollmentAssertRolledBack(t, first.DB, second.DB)
		})
	}
}

type observedEnrollmentData struct {
	*dml.Data
	enrolled   func()
	registered func() error
}

func (d *observedEnrollmentData) EnableCapturedExecutionGuards() error {
	if err := d.Data.EnableCapturedExecutionGuards(); err != nil {
		return err
	}
	if d.enrolled != nil {
		d.enrolled()
	}
	return nil
}
func (d *observedEnrollmentData) RegisterExecutionGuard(check func(context.Context) error) error {
	if d.registered != nil {
		if err := d.registered(); err != nil {
			return err
		}
	}
	return d.Data.RegisterExecutionGuard(check)
}

type openingEnrollmentSource struct {
	data    xhandler.Data
	db      *sql.DB
	entered chan struct{}
	release chan struct{}
}

func (s *openingEnrollmentSource) Open(ctx context.Context) (xhandler.Data, error) {
	close(s.entered)
	select {
	case <-s.release:
		return s.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *openingEnrollmentSource) InvocationKey() any { return s.db }

func TestCapturedEnrollmentJoinsOpeningOwnerBeforeRegistration(t *testing.T) {
	ctx := context.Background()
	first, second := sqlite.New(t), sqlite.New(t)
	for _, db := range []*sql.DB{first.DB, second.DB} {
		if _, err := db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
	}
	enrolled := make(chan struct{})
	var once sync.Once
	secondData := &observedEnrollmentData{Data: dml.NewData(second.DB), enrolled: func() { once.Do(func() { close(enrolled) }) }}
	firstData := &observedEnrollmentData{Data: dml.NewData(first.DB), registered: func() error {
		select {
		case <-enrolled:
			return nil
		default:
			return errors.New("guard acknowledged before opening owner enrolled")
		}
	}}
	root, _ := invocationDataScope(ctx, &guardOwnerSource{data: firstData, db: first.DB})
	if _, err := root.resolve(ctx); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	child, _ := invocationDataScope(withDataScope(ctx, root), &openingEnrollmentSource{data: secondData, db: second.DB, entered: entered, release: release})
	resolved := make(chan error, 1)
	go func() { _, err := child.resolve(ctx); resolved <- err }()
	<-entered
	registration := make(chan error, 1)
	go func() { registration <- root.registerExecutionGuard(ctx, func(context.Context) error { return nil }) }()
	close(release)
	if err := <-resolved; err != nil {
		t.Fatal(err)
	}
	if err := <-registration; err != nil {
		t.Fatal(err)
	}
	if err := root.complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

// Resolution admission stays open throughout enrollment. Joining the existing
// owner's once boundary must not use a WaitGroup.Wait at a zero-count transition.
func TestCapturedEnrollmentConcurrentZeroCountResolutions(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		ctx := context.Background()
		db := sqlite.New(t)
		root, _ := invocationDataScope(ctx, &guardOwnerSource{data: dml.NewData(db.DB), db: db.DB})
		if _, err := root.resolve(ctx); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		failures := make(chan error, 64)
		var concurrent sync.WaitGroup
		for index := 0; index < 64; index++ {
			concurrent.Add(1)
			go func(index int) {
				defer concurrent.Done()
				<-start
				if index%2 == 0 {
					_, err := root.resolve(ctx)
					failures <- err
				} else {
					failures <- root.registerExecutionGuard(ctx, func(context.Context) error { return nil })
				}
			}(index)
		}
		close(start)
		concurrent.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := root.complete(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
}
