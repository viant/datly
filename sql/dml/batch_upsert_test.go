package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqldialect "github.com/viant/sqlx/metadata/info/dialect"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

const audienceConflictSQL = "ON DUPLICATE KEY UPDATE WEIGHT = VALUES(WEIGHT), UPDATED_USER = VALUES(UPDATED_USER), UPDATED = CURRENT_TIMESTAMP"

type upsertWeight struct {
	Audience    int     `sqlx:"AUDIENCE_ID,primaryKey"`
	Creative    int     `sqlx:"CREATIVE_ID,primaryKey"`
	Weight      float64 `sqlx:"WEIGHT"`
	CreatedUser string  `sqlx:"CREATED_USER"`
	UpdatedUser string  `sqlx:"UPDATED_USER"`
}

// Observe the real SQLX builder/binder and database/sql transaction, without
// pretending that a mock establishes MySQL's conflict semantics.
type upsertDriver struct {
	queries                    []string
	args                       [][]driver.NamedValue
	begins, commits, rollbacks int
	active                     bool
	failTail                   bool
}

func (p *upsertDriver) Connect(context.Context) (driver.Conn, error) { return p, nil }
func (p *upsertDriver) Driver() driver.Driver                        { return p }
func (p *upsertDriver) Open(string) (driver.Conn, error)             { return p, nil }
func (p *upsertDriver) Close() error                                 { return nil }
func (p *upsertDriver) Begin() (driver.Tx, error)                    { p.begins++; p.active = true; return p, nil }
func (p *upsertDriver) Commit() error                                { p.commits++; p.active = false; return nil }
func (p *upsertDriver) Rollback() error                              { p.rollbacks++; p.active = false; return nil }
func (p *upsertDriver) Prepare(query string) (driver.Stmt, error)    { return &upsertStmt{p, query}, nil }
func (p *upsertDriver) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if !p.active {
		return nil, errors.New("execution outside owner transaction")
	}
	p.queries = append(p.queries, q)
	p.args = append(p.args, append([]driver.NamedValue(nil), args...))
	if p.failTail && strings.HasPrefix(q, "INSERT INTO `events`") {
		return nil, errors.New("event failure")
	}
	return upsertResult{}, nil
}

type upsertResult struct{}

func (upsertResult) LastInsertId() (int64, error) { return 0, nil }
func (upsertResult) RowsAffected() (int64, error) { return 1, nil }

type upsertStmt struct {
	p     *upsertDriver
	query string
}

func (s *upsertStmt) Close() error  { return nil }
func (s *upsertStmt) NumInput() int { return -1 }
func (s *upsertStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("unexpected query")
}
func (s *upsertStmt) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]driver.NamedValue, len(args))
	for i, v := range args {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.p.ExecContext(context.Background(), s.query, values)
}

func TestNativeBatchUpsertOrderBindingsAndTransaction(t *testing.T) {
	for _, n := range []int{2, 137} {
		for _, mode := range []string{"commit", "tail failure", "external"} {
			t.Run(fmt.Sprintf("%d/%s", n, mode), func(t *testing.T) {
				ctx := context.Background()
				p := &upsertDriver{failTail: mode == "tail failure"}
				db := sql.OpenDB(p)
				defer db.Close()
				var opts []Option
				var tx *sql.Tx
				if mode == "external" {
					var err error
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					opts = append(opts, WithTx(tx))
				}
				d := NewData(db, opts...)
				d.dialect = registry.LookupDialect(mysqlmeta.MySQL5())
				if err := d.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				rows := make([]*upsertWeight, n)
				for i := range rows {
					rows[i] = &upsertWeight{Audience: 11, Creative: i%2 + 1, Weight: float64(i), CreatedUser: "created", UpdatedUser: "updated"}
				}
				var affected int64
				if err := d.Update("prefix", &upsertWeight{Audience: 1, Creative: 1, Weight: 2}); err != nil {
					t.Fatal(err)
				}
				if err := d.Delete("prefix", &upsertWeight{Audience: 1, Creative: 2}); err != nil {
					t.Fatal(err)
				}
				if err := d.InsertWithBatchSizeAndOnDuplicateKeySQL("weights", rows, n, &affected, audienceConflictSQL); err != nil {
					t.Fatal(err)
				}
				if err := d.Insert("events", &upsertWeight{Audience: 1, Creative: 1, Weight: 1}); err != nil {
					t.Fatal(err)
				}
				if len(p.queries) != 0 || !d.queue[2].appendBarrier || d.queue[2].batchSize != n {
					t.Fatal("queue contract changed")
				}
				err := d.Complete(ctx, nil)
				if (err != nil) != (mode == "tail failure") {
					t.Fatalf("completion: %v", err)
				}
				if len(p.queries) != 4 || !strings.HasPrefix(p.queries[0], "UPDATE ") || !strings.HasPrefix(p.queries[1], "DELETE ") || !strings.HasPrefix(p.queries[3], "INSERT INTO `events`") {
					t.Fatalf("order=%v", p.queries)
				}
				q := p.queries[2]
				if !strings.HasSuffix(q, audienceConflictSQL) || strings.Count(q, "?") != n*5 || !strings.Contains(q, "(WEIGHT,CREATED_USER,UPDATED_USER,AUDIENCE_ID,CREATIVE_ID)") {
					t.Fatalf("unexpected SQLX upsert: %s", q)
				}
				if len(p.args[2]) != n*5 {
					t.Fatal("bindings changed")
				}
				for i := range rows {
					got := p.args[2][i*5 : (i+1)*5]
					if got[3].Value != int64(11) || got[4].Value != int64(i%2+1) || got[0].Value != float64(i) || got[1].Value != "created" || got[2].Value != "updated" {
						t.Fatalf("row %d bindings=%v", i, got)
					}
				}
				if p.begins != 1 {
					t.Fatal("multiple transactions")
				}
				if mode == "external" {
					if p.commits != 0 || p.rollbacks != 0 || !p.active {
						t.Fatal("caller transaction completed")
					}
					if _, err = tx.ExecContext(ctx, "caller"); err != nil {
						t.Fatal(err)
					}
				} else if mode == "tail failure" {
					if p.rollbacks != 1 || p.commits != 0 {
						t.Fatal("failure did not roll back entire transaction")
					}
				} else if p.commits != 1 || p.rollbacks != 0 {
					t.Fatal("owner did not commit")
				}
			})
		}
	}
}

func TestNativeBatchUpsertConstructorCacheIsolation(t *testing.T) {
	p := &upsertDriver{}
	db := sql.OpenDB(p)
	defer db.Close()
	d := NewData(db)
	d.dialect = registry.LookupDialect(mysqlmeta.MySQL5())
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	suffixes := []string{"", audienceConflictSQL, "", "ON DUPLICATE KEY UPDATE WEIGHT = 7", audienceConflictSQL, ""}
	var affected int64
	for _, suffix := range suffixes {
		rows := []*upsertWeight{{1, 2, 3, "c", "u"}}
		if err := d.InsertWithBatchSizeAndOnDuplicateKeySQL("weights", rows, 1, &affected, suffix); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.InsertWithBatchSizeAndOnDuplicateKeySQL("weights", []*upsertWeight{}, 1, &affected, audienceConflictSQL); err != nil {
		t.Fatal(err)
	}
	if len(d.queue) != len(suffixes) {
		t.Fatal("empty input queued")
	}
	if err := d.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(p.queries) != len(suffixes) {
		t.Fatal("operations merged")
	}
	for i, suffix := range suffixes {
		q := p.queries[i]
		if suffix == "" {
			if strings.Contains(q, "ON DUPLICATE") {
				t.Fatal("suffix leaked into plain insert")
			}
		} else if !strings.HasSuffix(q, suffix) {
			t.Fatalf("suffix %d changed: %s", i, q)
		}
	}
	if len(d.insertServices) != 1 {
		t.Fatal("suffixed service retained in plain cache")
	}
}

func TestNativeBatchUpsertFailureRollsBackPrefixAndRows(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE weights(AUDIENCE_ID INTEGER,CREATIVE_ID INTEGER,WEIGHT REAL,CREATED_USER TEXT,UPDATED_USER TEXT,PRIMARY KEY(AUDIENCE_ID,CREATIVE_ID))`, `CREATE TABLE events(id INTEGER PRIMARY KEY)`, `INSERT INTO weights VALUES(1,1,2,'original','old')`); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	dialect, err := d.dialectFor(ctx, h.DB)
	if err != nil {
		t.Fatal(err)
	}
	forced := *dialect
	forced.Insert = sqldialect.InsertWithMultiValues
	d.dialect = &forced
	if err = d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err = d.Update("weights", &upsertWeight{1, 1, 3, "original", "prefix"}); err != nil {
		t.Fatal(err)
	}
	var affected int64
	// SQLite verifies actual rollback, while the driver test verifies the exact
	// original MySQL suffix. MySQL conflict semantics have their own opt-in test.
	if err = d.InsertWithBatchSizeAndOnDuplicateKeySQL("weights", []*upsertWeight{{1, 1, 7, "replacement", "upsert"}, {1, 2, 9, "new", "new"}}, 2, &affected, "ON CONFLICT(AUDIENCE_ID,CREATIVE_ID) DO UPDATE SET WEIGHT=excluded.WEIGHT,UPDATED_USER=excluded.UPDATED_USER"); err != nil {
		t.Fatal(err)
	}
	if err = d.Execute("INSERT INTO missing_event_table VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = d.Complete(ctx, nil); err == nil {
		t.Fatal("late failure accepted")
	}
	var count int
	var weight float64
	var updater string
	if err = h.DB.QueryRow("SELECT COUNT(*) FROM weights").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err = h.DB.QueryRow("SELECT WEIGHT,UPDATED_USER FROM weights WHERE AUDIENCE_ID=1 AND CREATIVE_ID=1").Scan(&weight, &updater); err != nil {
		t.Fatal(err)
	}
	if count != 1 || weight != 2 || updater != "old" {
		t.Fatalf("prefix/upsert escaped rollback: count=%d weight=%v updater=%s", count, weight, updater)
	}
}
