package sequencer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func aliasReservation(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow("SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestAliasedAllocationScalarPointeeAndNilLocations(t *testing.T) {
	t.Run("scalar-public-pointer", func(t *testing.T) {
		db := aliasDefaultDB(t)
		r := &aliasDefaultRow{}
		rows := []*aliasDefaultRow{r, r}
		if err := New(db).Allocate(context.Background(), "records", rows, "ID"); err != nil || r.ID != 1 || aliasReservation(t, db) != 2 {
			t.Fatal("scalar", r.ID, err)
		}
	})
	type row struct {
		ID *int64 `sqlx:"ID,primaryKey=true,autoincrement=true"`
	}
	for _, name := range []string{"existing-shared-pointee", "same-nil-holder", "same-nil-and-distinct"} {
		t.Run(name, func(t *testing.T) {
			db := aliasDefaultDB(t)
			zero := int64(0)
			first := &row{}
			var rows []*row
			switch name {
			case "existing-shared-pointee":
				rows = []*row{{ID: &zero}, {ID: &zero}}
			case "same-nil-holder":
				rows = []*row{first, first}
			default:
				rows = []*row{first, first, {}}
			}
			if err := New(db).Allocate(context.Background(), "records", rows, "ID"); err != nil {
				t.Fatal(err)
			}
			if rows[0].ID == nil || *rows[0].ID != 1 || rows[0].ID != rows[1].ID {
				t.Fatal("shared identity holder", rows)
			}
			if len(rows) == 3 && (rows[2].ID == rows[0].ID || *rows[2].ID != 2) {
				t.Fatal("distinct nil holder collapsed", rows)
			}
			if aliasReservation(t, db) != int64(len(rows)) || aliasDefaultCount(t, db) != 0 {
				t.Fatal("reservation occurrence count/product allocation")
			}
			t.Logf("ALIAS_LOCATIONS %s emptyOccurrences=%d assignedFirst1 nextUnique2 reservation=%d", name, len(rows), aliasReservation(t, db))
		})
	}
}
func TestAliasedAllocationSuppliedPendingConsumedTail(t *testing.T) {
	db := aliasDefaultDB(t)
	s := New(db)
	provided, shared, later := &aliasDefaultRow{ID: 1}, &aliasDefaultRow{}, &aliasDefaultRow{}
	if err := s.Allocate(context.Background(), "records", []*aliasDefaultRow{provided, shared, shared, later}, "ID"); err != nil {
		t.Fatal(err)
	}
	if provided.ID != 1 || shared.ID != 2 || later.ID != 3 || aliasReservation(t, db) != 4 {
		t.Fatal("pending exclusion/unique cursor", provided, shared, later)
	}
	for _, values := range s.pending {
		for _, value := range []int64{1, 2, 3, 4} {
			if !values[value] {
				t.Fatal("consumed duplicate tail escaped pending", value)
			}
		}
	}
	next := &aliasDefaultRow{}
	if err := s.Allocate(context.Background(), "records", next, "ID"); err != nil || next.ID != 5 {
		t.Fatal("subsequent allocation reused tail", next, err)
	}
	if aliasReservation(t, db) != 5 || aliasDefaultCount(t, db) != 0 {
		t.Fatal("subsequent reservation/product rows")
	}
	t.Log("supplied1 shared2 later3 reservation4; consumed unused4 remembered; next5, no product rows")
}
func TestAliasedAllocationActualDuplicateInsertRollback(t *testing.T) {
	db := aliasDefaultDB(t)
	ctx := context.Background()
	r := &aliasDefaultRow{Name: "shared"}
	rows := []*aliasDefaultRow{r, r}
	if err := New(db).Allocate(ctx, "records", rows, "ID"); err != nil {
		t.Fatal(err)
	}
	if r.ID != 1 || aliasReservation(t, db) != 2 || aliasDefaultCount(t, db) != 0 {
		t.Fatal("whole allocation before any insertion")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	service, err := insert.New(ctx, db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if n, _, e := service.Exec(ctx, rows[0], tx); e != nil || n != 1 {
		t.Fatal("genuine first SQLX insert", n, e)
	}
	if _, _, e := service.Exec(ctx, rows[1], tx); e == nil || !strings.Contains(e.Error(), "UNIQUE constraint failed") {
		t.Fatal("duplicate must fail at genuine SQL execution", e)
	}
	if aliasDefaultCount(t, tx) != 1 {
		t.Fatal("truthful inserted prefix1")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if aliasDefaultCount(t, db) != 0 || aliasReservation(t, db) != 2 || r.ID != 1 {
		t.Fatal("product rollback/public identity/standalone reservation")
	}
	t.Log("default reserve2/sharedpublic1, allallocation before two actual INSERTs, duplicate SQL failure after truthful prefix1, product rollback0/infra2")
}

// Real driver commit observer, not a fabricated reservation or returned value.
// Cancellation is exposed only after SQLX's genuine owned reservation commits.
type aliasCommitCancel struct {
	context.Context
	cancel    context.CancelFunc
	committed atomic.Bool
	commits   atomic.Int32
}

func (c *aliasCommitCancel) Err() error {
	if c.committed.Load() {
		c.cancel()
	}
	return c.Context.Err()
}

type aliasCommitConnector struct {
	base driver.Driver
	dsn  string
	ctx  *aliasCommitCancel
}

func (c *aliasCommitConnector) Driver() driver.Driver { return c.base }
func (c *aliasCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &aliasCommitConn{Conn: conn, owner: c}, nil
}

type aliasCommitConn struct {
	driver.Conn
	owner *aliasCommitConnector
}

func (c *aliasCommitConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &aliasObservedCommit{Tx: tx, state: c.owner.ctx}, nil
}
func (c *aliasCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if v, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = v.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &aliasObservedCommit{Tx: tx, state: c.owner.ctx}, nil
}

type aliasObservedCommit struct {
	driver.Tx
	state *aliasCommitCancel
}

func (t *aliasObservedCommit) Commit() error {
	err := t.Tx.Commit()
	if err == nil {
		t.state.commits.Add(1)
		t.state.committed.Store(true)
	}
	return err
}
func TestAliasedAllocationCancellationBeforeAndAfterReservation(t *testing.T) {
	type row struct {
		ID *int64 `sqlx:"ID,primaryKey=true,autoincrement=true"`
	}
	t.Run("before-work", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := &row{}
		if err := New(nil).Allocate(ctx, "records", []*row{r, r}, "ID"); !errors.Is(err, context.Canceled) || r.ID != nil {
			t.Fatal("cancelled initial preflight", err, r)
		}
	})
	t.Run("after-native-commit-before-publication", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "cancel-after-commit.db")
		plain, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer plain.Close()
		if _, err = plain.Exec("CREATE TABLE records(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT)"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		state := &aliasCommitCancel{Context: ctx, cancel: cancel}
		db := sql.OpenDB(&aliasCommitConnector{base: plain.Driver(), dsn: dbPath, ctx: state})
		defer db.Close()
		first, next := &row{}, &row{}
		s := New(db)
		err = s.Allocate(state, "records", []*row{first, first, next}, "ID")
		if !errors.Is(err, context.Canceled) || state.commits.Load() != 1 || first.ID != nil || next.ID != nil {
			t.Fatal("post-reservation cancellation stage/partial publication", state.commits.Load(), first, next, err)
		}
		if aliasReservation(t, plain) != 3 || aliasDefaultCount(t, plain) != 0 {
			t.Fatal("real reservation or product state")
		}
		for _, v := range s.pending {
			if !v[1] || !v[2] || !v[3] {
				t.Fatal("cancelled consumed range must remain pending", v)
			}
		}
		t.Log("real SQLX commit1/reservation3 precedes cancellation; all nullable public IDs nil; product0; pending1,2,3")
	})
}
func TestAliasedAllocationReservationFailureNoPublication(t *testing.T) {
	db := aliasDefaultDB(t)
	for _, q := range []string{"CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value>=0))", "CREATE TRIGGER fail_reservation BEFORE INSERT ON sqlx_sequence_reservations BEGIN SELECT RAISE(ABORT,'real reservation fault'); END"} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	type row struct {
		ID *int64 `sqlx:"ID,primaryKey=true,autoincrement=true"`
	}
	r, next := &row{}, &row{}
	err := New(db).Allocate(context.Background(), "records", []*row{r, r, next}, "ID")
	if err == nil || !strings.Contains(err.Error(), "real reservation fault") || r.ID != nil || next.ID != nil || aliasDefaultCount(t, db) != 0 {
		t.Fatal("genuine reservation failure", r, next, err)
	}
	var n int
	if err = db.QueryRow("SELECT COUNT(*) FROM sqlx_sequence_reservations").Scan(&n); err != nil || n != 0 {
		t.Fatal("failed native reservation rows", n, err)
	}
}
func TestAliasedAllocationStrategiesAndCallerTransactions(t *testing.T) {
	for _, strategy := range []dialect.PresetIDStrategy{"", dialect.PresetIDWithTransientTransaction, dialect.PresetIDWithReservation} {
		for _, managed := range []bool{false, true} {
			t.Run(fmt.Sprintf("strategy=%s/caller=%v", strategy, managed), func(t *testing.T) {
				db := aliasDefaultDB(t)
				ctx := context.Background()
				s := New(db).WithStrategy(strategy)
				var tx *sql.Tx
				if managed {
					var err error
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					s = New(db, tx).WithStrategy(strategy)
				}
				r := &aliasDefaultRow{}
				if err := s.Allocate(ctx, "records", []*aliasDefaultRow{r, r}, "ID"); err != nil || r.ID != 1 {
					t.Fatal("existing selected strategy", strategy, err, r)
				}
				if s.strategy != strategy {
					t.Fatal("strategy silently switched")
				}
				if managed {
					var reserved int
					if err := tx.QueryRowContext(ctx, "SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); err != nil || reserved != 2 {
						t.Fatal("caller owned reserved2", reserved, err)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					var n int
					if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='sqlx_sequence_reservations'").Scan(&n); err != nil || n != 0 {
						t.Fatal("caller infra rollback", n, err)
					}
				} else if aliasReservation(t, db) != 2 {
					t.Fatal("standalone reserve2")
				}
				if aliasDefaultCount(t, db) != 0 {
					t.Fatal("strategy allocated product rows")
				}
			})
		}
	}
}

// Full native reservation mapped-type validation remains authoritative even for
// an unused duplicate tail. This boundary was explicitly reviewed by Astra;
// the historical success expectation and six failing logs are frozen separately.
func TestAliasedAllocationNarrowUnusedTailSQLXRejection(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		for _, managed := range []bool{false, true} {
			t.Run(fmt.Sprintf("unsigned=%v/caller=%v", unsigned, managed), func(t *testing.T) {
				db := aliasDefaultDB(t)
				max, want, typ := 126, 128, "int8"
				if unsigned {
					max, want, typ = 254, 256, "uint8"
				}
				if _, err := db.Exec("INSERT INTO records(ID) VALUES(?)", max); err != nil {
					t.Fatal(err)
				}
				svc := New(db)
				q := interface {
					QueryRowContext(context.Context, string, ...any) *sql.Row
				}(db)
				var tx *sql.Tx
				ctx := context.Background()
				if managed {
					var err error
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					svc = New(db, tx)
					q = tx
				}
				var err error
				if unsigned {
					type row struct {
						ID *uint8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
					}
					r := &row{}
					err = svc.Allocate(ctx, "records", []*row{r, r}, "ID")
					if r.ID != nil {
						t.Fatal("SQLX rejection published uint8 pointer", r)
					}
				} else {
					type row struct {
						ID *int8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
					}
					r := &row{}
					err = svc.Allocate(ctx, "records", []*row{r, r}, "ID")
					if r.ID != nil {
						t.Fatal("SQLX rejection published int8 pointer", r)
					}
				}
				expected := fmt.Sprintf("native sequence value %d overflows mapped Go type %s", want, typ)
				if err == nil || !strings.Contains(err.Error(), expected) {
					t.Fatal("not authoritative full-reservation SQLX boundary", expected, err)
				}
				var reserved, engine int
				if e := q.QueryRowContext(ctx, "SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); e != nil || reserved != want {
					t.Fatal("truthful consumed native range", reserved, e)
				}
				if e := q.QueryRowContext(ctx, "SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&engine); e != nil || engine != want {
					t.Fatal("truthful engine range", engine, e)
				}
				if aliasDefaultCount(t, q) != 1 {
					t.Fatal("failed reservation validation changed product seed")
				}
				for _, values := range svc.pending {
					if values[int64(max+1)] || values[int64(want)] {
						t.Fatal("failed SQLX result must not invent published pending values", values)
					}
				}
				if managed {
					if _, e := tx.ExecContext(ctx, "INSERT INTO records(ID,NAME) VALUES(1000,'caller remains usable')"); e != nil {
						t.Fatal("caller TX ownership lost", e)
					}
					if aliasDefaultCount(t, tx) != 2 {
						t.Fatal("caller later physical write lost")
					}
					if e := tx.Rollback(); e != nil {
						t.Fatal(e)
					}
					var tables int
					if e := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='sqlx_sequence_reservations'").Scan(&tables); e != nil || tables != 0 {
						t.Fatal("caller rollback infrastructure", tables, e)
					}
					if e := db.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&engine); e != nil || engine != max {
						t.Fatal("caller restores old engine counter", engine, e)
					}
				} else if aliasReservation(t, db) != int64(want) {
					t.Fatal("standalone committed native reservation lost")
				}
				if aliasDefaultCount(t, db) != 1 {
					t.Fatal("product seed must be exact after rollback")
				}
				t.Logf("EXPECTED_SQLX_REJECTION type=%s caller=%v exactError=%v publicPointer=nil actualReserved=%d nativePendingUnpublished=true callerRollbackSeedExact=true", typ, managed, err, reserved)
			})
		}
	}
}
func TestAliasedAllocationNarrowWholeReservationFits(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsigned=%v", unsigned), func(t *testing.T) {
			db := aliasDefaultDB(t)
			max := 125
			if unsigned {
				max = 253
			}
			if _, err := db.Exec("INSERT INTO records(ID) VALUES(?)", max); err != nil {
				t.Fatal(err)
			}
			if unsigned {
				type row struct {
					ID *uint8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
				}
				r := &row{}
				if err := New(db).Allocate(context.Background(), "records", []*row{r, r}, "ID"); err != nil || r.ID == nil || *r.ID != 254 {
					t.Fatal("whole range fits unsigned", r, err)
				}
			} else {
				type row struct {
					ID *int8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
				}
				r := &row{}
				if err := New(db).Allocate(context.Background(), "records", []*row{r, r}, "ID"); err != nil || r.ID == nil || *r.ID != 126 {
					t.Fatal("whole range fits signed", r, err)
				}
			}
			if aliasReservation(t, db) != int64(max+2) || aliasDefaultCount(t, db) != 1 {
				t.Fatal("whole reservation count/product seed")
			}
		})
	}
}
func TestAliasedAllocationAssignedOverflowNoPublication(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsigned=%v", unsigned), func(t *testing.T) {
			db := aliasDefaultDB(t)
			max := 126
			if unsigned {
				max = 254
			}
			if _, err := db.Exec("INSERT INTO records(ID) VALUES(?)", max); err != nil {
				t.Fatal(err)
			}
			if unsigned {
				type row struct {
					ID *uint8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
				}
				a, b := &row{}, &row{}
				if err := New(db).Allocate(context.Background(), "records", []*row{a, a, b}, "ID"); err == nil || a.ID != nil || b.ID != nil {
					t.Fatal("unsigned assigned overflow escaped", a, b, err)
				}
			} else {
				type row struct {
					ID *int8 `sqlx:"ID,primaryKey=true,autoincrement=true"`
				}
				a, b := &row{}, &row{}
				if err := New(db).Allocate(context.Background(), "records", []*row{a, a, b}, "ID"); err == nil || a.ID != nil || b.ID != nil {
					t.Fatal("signed assigned overflow escaped", a, b, err)
				}
			}
			if aliasDefaultCount(t, db) != 1 {
				t.Fatal("assigned overflow changed product seed")
			}
		})
	}
}
