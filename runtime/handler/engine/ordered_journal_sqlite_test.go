//go:build sqlite_trace

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	xh "github.com/viant/xdatly/handler"
)

type orderedRow struct {
	ID    int    `sqlx:"ID,primaryKey"`
	Label string `sqlx:"LABEL"`
}
type orderedTrace struct {
	mu     sync.Mutex
	lines  []string
	active bool
}

func (r *orderedTrace) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		r.lines = append(r.lines, s)
	}
}
func (r *orderedTrace) copy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lines...)
}

var orderedDBSerial atomic.Uint64

func orderedDB(t *testing.T, r *orderedTrace, name, failLabel string, commitFail bool) *sql.DB {
	t.Helper()
	driverName := fmt.Sprintf("sqlite_ordered_%d", orderedDBSerial.Add(1))
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		return conn.SetTrace(&sqlite3.TraceConfig{EventMask: sqlite3.TraceStmt, WantExpandedSQL: true, Callback: func(info sqlite3.TraceInfo) int {
			text := info.ExpandedSQL
			if text == "" {
				text = info.StmtOrTrigger
			}
			upper := strings.ToUpper(strings.TrimSpace(text))
			if upper == "BEGIN" || upper == "COMMIT" || upper == "ROLLBACK" || strings.HasPrefix(upper, "INSERT INTO RECORDS") || strings.HasPrefix(upper, "INSERT INTO EVENTS") {
				r.add(name + " " + text)
			}
			return 0
		}})
	}})
	db, err := sql.Open(driverName, filepath.Join(t.TempDir(), name+".db")+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{"CREATE TABLE guard (ID INTEGER PRIMARY KEY)", "INSERT INTO guard VALUES(1)", "CREATE TABLE records(ID INTEGER PRIMARY KEY,LABEL TEXT,GUARD INTEGER DEFAULT 1 REFERENCES guard(ID) DEFERRABLE INITIALLY DEFERRED)", "CREATE TABLE events(ID INTEGER PRIMARY KEY,LABEL TEXT)", "CREATE TABLE effects(LABEL TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO effects VALUES(NEW.LABEL);END"} {
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if failLabel != "" {
		body := "SELECT RAISE(ABORT,'ordered statement failure');"
		if commitFail {
			body = "UPDATE records SET GUARD=999 WHERE ID=NEW.ID;"
		}
		q := "CREATE TRIGGER injected AFTER INSERT ON records WHEN NEW.LABEL='" + failLabel + "' BEGIN " + body + "END"
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
func orderedData(t *testing.T, scope *dataScope) xh.Data {
	t.Helper()
	data, err := scope.resolve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func orderedInsert(t *testing.T, data xh.Data, id int, label string) {
	t.Helper()
	if err := data.(interface {
		InsertWithQueueContract(string, any, rh.QueueContract) error
	}).InsertWithQueueContract("records", &orderedRow{ID: id, Label: label}, rh.SourceRow); err != nil {
		t.Fatal(err)
	}
}
func orderedChild(t *testing.T, parent *dataScope, source dml.Source, relation ComponentRelation, order string) *dataScope {
	t.Helper()
	scope, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), parent), relation, order), source)
	return scope
}
func physicalLabels(lines []string) []string {
	var result []string
	pattern := regexp.MustCompile(`'([^']*)'`)
	for _, line := range lines {
		if strings.Contains(line, " INSERT INTO records(") || strings.Contains(line, " INSERT INTO records (") {
			for _, match := range pattern.FindAllStringSubmatch(line, -1) {
				result = append(result, line[:1]+":"+match[1])
			}
		}
	}
	return result
}
func physicalTerminal(lines []string, op string) []string {
	var result []string
	for _, line := range lines {
		if line == line[:1]+" "+op {
			result = append(result, line[:1])
		}
	}
	return result
}
func orderedCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Physical SQLite trace and triggers distinguish journal admissions, actual
// statement execution, transaction creation and durable effects independently.
func TestOrderedJournalPhysicalCompletion(t *testing.T) {
	cases := []struct {
		name, failOwner, failLabel                        string
		commitFail, reverseStart, finalize, finalizerFail bool
	}{
		{name: "success"}, {name: "reverse-start", reverseStart: true}, {name: "early-preparation", finalize: true}, {name: "finalizer-failure", finalize: true, finalizerFail: true},
		{name: "first-root-failure", failOwner: "A", failLabel: "root1"}, {name: "changelog-failure", failOwner: "B", failLabel: "log1"}, {name: "child-failure", failOwner: "A", failLabel: "child1"}, {name: "second-root-failure", failOwner: "A", failLabel: "root2"}, {name: "second-log-failure", failOwner: "B", failLabel: "log2"}, {name: "second-child-failure", failOwner: "A", failLabel: "child2"},
		{name: "first-commit-failure", failOwner: "A", failLabel: "root1", commitFail: true}, {name: "later-commit-failure", failOwner: "B", failLabel: "log2", commitFail: true}, {name: "reverse-first-commit-failure", failOwner: "B", failLabel: "log2", commitFail: true, reverseStart: true}, {name: "reverse-later-commit-failure", failOwner: "A", failLabel: "root1", commitFail: true, reverseStart: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &orderedTrace{}
			labelA, labelB := "", ""
			if tc.failOwner == "A" {
				labelA = tc.failLabel
			}
			if tc.failOwner == "B" {
				labelB = tc.failLabel
			}
			a, b := orderedDB(t, r, "A", labelA, tc.commitFail), orderedDB(t, r, "B", labelB, tc.commitFail)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			if err := root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			main := orderedData(t, root)
			// An empty B binding opens a transaction independently of operation order.
			starter := orderedChild(t, root, dml.Source{DB: b}, ComponentBinding, "00")
			bd := orderedData(t, starter)
			r.active = true
			if tc.reverseStart {
				if err := bd.(xh.TransactionStarter).Start(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err := main.(xh.TransactionStarter).Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			starter.seal()
			want := []string{"A:root1", "B:log1", "A:child1", "A:root2", "B:log2", "A:child2"}
			var admissions []string
			for i := 1; i <= 2; i++ {
				orderedInsert(t, main, i, fmt.Sprintf("root%d", i))
				admissions = append(admissions, fmt.Sprintf("A:root%d", i))
				child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
				foreign := orderedData(t, child)
				orderedInsert(t, foreign, i, fmt.Sprintf("log%d", i))
				admissions = append(admissions, fmt.Sprintf("B:log%d", i))
				nested := orderedChild(t, child, dml.Source{DB: a}, ComponentBufferedImperative, "")
				orderedInsert(t, orderedData(t, nested), 10+i, fmt.Sprintf("child%d", i))
				admissions = append(admissions, fmt.Sprintf("A:child%d", i))
				nested.seal()
				child.seal()
			}
			if len(physicalLabels(r.copy())) != 0 {
				t.Fatal("product statements drained between component calls")
			}
			if !reflect.DeepEqual(admissions, want) {
				t.Fatal(admissions)
			}
			var cause error
			if tc.finalize {
				cause = root.prepareProtectedFinalization(t.Context())
				if cause != nil {
					t.Fatal(cause)
				}
				if got := physicalLabels(r.copy()); !reflect.DeepEqual(got, want) {
					t.Fatalf("prepared=%v want=%v", got, want)
				}
				if len(physicalTerminal(r.copy(), "COMMIT")) != 0 {
					t.Fatal("preparation committed")
				}
				if tc.finalizerFail {
					cause = fmt.Errorf("finalizer failed")
				}
			}
			err := root.complete(t.Context(), cause)
			if (err != nil) != (tc.failOwner != "" || tc.finalizerFail) {
				t.Fatalf("completion=%v", err)
			}
			got := physicalLabels(r.copy())
			expected := want
			if tc.failOwner != "" && !tc.commitFail {
				for n, value := range want {
					if value == tc.failOwner+":"+tc.failLabel {
						expected = want[:n+1]
						break
					}
				}
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("physical prefix=%v want=%v\n%s", got, expected, strings.Join(r.copy(), "\n"))
			}
			txOrder := []string{"A", "B"}
			if tc.reverseStart {
				txOrder = []string{"B", "A"}
			}
			if tc.failOwner != "" && !tc.commitFail || tc.finalizerFail {
				if got := physicalTerminal(r.copy(), "COMMIT"); len(got) != 0 {
					t.Fatalf("failed work committed %v", got)
				}
				rollbackOrder := []string{txOrder[1], txOrder[0]}
				if tc.failLabel == "root1" && !tc.commitFail && !tc.reverseStart {
					rollbackOrder = []string{"A"}
				}
				if got := physicalTerminal(r.copy(), "ROLLBACK"); !reflect.DeepEqual(got, rollbackOrder) {
					t.Fatalf("rollback=%v", got)
				}
			} else {
				commits := txOrder
				if tc.commitFail && tc.failOwner == txOrder[0] {
					commits = txOrder[:1]
				}
				if got := physicalTerminal(r.copy(), "COMMIT"); !reflect.DeepEqual(got, commits) {
					t.Fatalf("commits=%v want=%v", got, commits)
				}
			}
			wantA, wantB := 4, 2
			if tc.failOwner != "" || tc.finalizerFail {
				wantA, wantB = 0, 0
				if tc.commitFail && tc.failOwner == txOrder[1] {
					if txOrder[0] == "A" {
						wantA = 4
					} else {
						wantB = 2
					}
				}
			}
			if n := orderedCount(t, a, "records"); n != wantA {
				t.Fatalf("A durable=%d want=%d", n, wantA)
			}
			if n := orderedCount(t, b, "records"); n != wantB {
				t.Fatalf("B durable=%d want=%d", n, wantB)
			}
			if orderedCount(t, a, "effects") != wantA || orderedCount(t, b, "effects") != wantB {
				t.Fatal("trigger effects differ from durable rows")
			}
			outcome := root.completionOutcome()
			if len(outcome.Transactions) != 2 || outcome.Transactions[0].Unit != 0 || outcome.Transactions[1].Unit != 1 || outcome.Transactions[0].State != root.data.(*dml.Data).TransactionOutcome().State || outcome.Transactions[1].State != root.units[0].data.(*dml.Data).TransactionOutcome().State {
				t.Fatalf("per-owner identity changed with physical transaction order: %+v", outcome)
			}

			t.Logf("admissions=%v physical=%v tx=%v\n%s", admissions, got, txOrder, strings.Join(r.copy(), "\n"))
		})
	}
}

func TestOrderedJournalTopology(t *testing.T) {
	r := &orderedTrace{}
	a, b, c := orderedDB(t, r, "A", "", false), orderedDB(t, r, "B", "", false), orderedDB(t, r, "C", "", false)
	root := neutralDataScope()
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	// Reverse completion deliberately differs from declared binding order.
	first := orderedChild(t, root, dml.Source{DB: a}, ComponentBinding, "01:00000")
	second := orderedChild(t, root, dml.Source{DB: b}, ComponentBinding, "02:00000")
	repeated := orderedChild(t, root, dml.Source{DB: c}, ComponentBinding, "01:00001")
	orderedInsert(t, orderedData(t, second), 1, "second")
	second.seal()
	orderedInsert(t, orderedData(t, repeated), 1, "repeat")
	repeated.seal()
	orderedInsert(t, orderedData(t, first), 1, "first")
	empty := orderedChild(t, first, dml.Source{DB: b}, ComponentBufferedImperative, "")
	orderedData(t, empty)
	empty.seal()
	nested := orderedChild(t, first, dml.Source{DB: c}, ComponentBufferedImperative, "")
	orderedInsert(t, orderedData(t, nested), 2, "nested")
	nested.seal()
	first.seal()
	// A retained sealed frame dispatches as a sibling at nearest open ancestor.
	sibling := orderedChild(t, nested, dml.Source{DB: a}, ComponentBufferedImperative, "")
	orderedInsert(t, orderedData(t, sibling), 2, "sibling")
	sibling.seal()
	r.active = true
	if err := root.complete(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"A:sibling", "A:first", "C:nested", "C:repeat", "B:second"}
	if got := physicalLabels(r.copy()); !reflect.DeepEqual(got, want) {
		t.Fatalf("physical=%v want=%v\n%s", got, want, strings.Join(r.copy(), "\n"))
	}
	if len(root.units) != 3 {
		t.Fatalf("owners=%d", len(root.units))
	}
}

func TestOrderedJournalEngineLateEnrollment(t *testing.T) {
	r := &orderedTrace{}
	a, b := orderedDB(t, r, "A", "", false), orderedDB(t, r, "B", "", false)
	r.active = true
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: a}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
		v, _, err := in.Binder.Lookup(ctx, xh.DataKey)
		if err != nil {
			return nil, err
		}
		data := v.(xh.Data)
		if err = data.Insert("records", &orderedRow{ID: 1, Label: "before"}); err != nil {
			return nil, err
		}
		_, err = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: b}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
			v, _, err := in.Binder.Lookup(ctx, xh.DataKey)
			if err != nil {
				return nil, err
			}
			return nil, v.(xh.Data).Insert("records", &orderedRow{ID: 1, Label: "child"})
		})})
		if err != nil {
			return nil, err
		}
		if len(physicalLabels(r.copy())) != 0 {
			return nil, fmt.Errorf("intermediate product drain")
		}
		return nil, data.Insert("records", &orderedRow{ID: 2, Label: "after"})
	})})
	if err != nil {
		t.Fatal(err)
	}
	if got := physicalLabels(r.copy()); !reflect.DeepEqual(got, []string{"A:before", "B:child", "A:after"}) {
		t.Fatal(got)
	}
}

func TestOrderedJournalNativeBatchBarriers(t *testing.T) {
	r := &orderedTrace{}
	a, b := orderedDB(t, r, "A", "", false), orderedDB(t, r, "B", "", false)
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad := orderedData(t, root)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ad.Insert("records", &orderedRow{ID: 1, Label: "a1"}))
	must(ad.Insert("records", &orderedRow{ID: 2, Label: "a2"}))
	child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
	bd := orderedData(t, child)
	must(bd.Insert("records", &orderedRow{ID: 1, Label: "b1"}))
	child.seal()
	must(ad.Insert("records", &orderedRow{ID: 3, Label: "a3"}))
	must(ad.Insert("records", &orderedRow{ID: 4, Label: "a4"}))
	// One queued slice retains one native operation and its payload barrier.
	event := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
	ed := orderedData(t, event)
	must(ed.(interface {
		InsertWithQueueContract(string, any, rh.QueueContract) error
	}).InsertWithQueueContract("events", []*orderedRow{{ID: 1, Label: "event1"}, {ID: 2, Label: "event2"}}, rh.SourceSlice))
	event.seal()
	r.active = true
	must(root.complete(t.Context(), nil))
	want := []string{"A:a1", "A:a2", "B:b1", "A:a3", "A:a4"}
	if got := physicalLabels(r.copy()); !reflect.DeepEqual(got, want) {
		t.Fatalf("labels=%v", got)
	}
	insertsA := 0
	for _, line := range r.copy() {
		if strings.HasPrefix(line, "A INSERT INTO records") {
			insertsA++
			if strings.Contains(line, "a1") && strings.Contains(line, "a3") {
				t.Fatal("batch crossed foreign owner")
			}
		}
	}
	if insertsA != 2 {
		t.Fatalf("adjacent native batches=%d want=2", insertsA)
	}
	if orderedCount(t, b, "events") != 2 {
		t.Fatal("source slice lost rows")
	}
	// The operation report preserves slice admission instead of fabricating rows.
	unit := event.unit.data.(*dml.Data)
	if report := unit.MutationReport(); report.Queued != 2 {
		t.Fatalf("B native identities=%d want=2", report.Queued)
	}
	t.Log(strings.Join(r.copy(), "\n"))
}

func TestOrderedJournalCommitFailureForwardCleanup(t *testing.T) {
	r := &orderedTrace{}
	a, b, c := orderedDB(t, r, "A", "", false), orderedDB(t, r, "B", "b", true), orderedDB(t, r, "C", "", false)
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	orderedInsert(t, orderedData(t, root), 1, "a")
	for _, item := range []struct {
		db    *sql.DB
		label string
	}{{b, "b"}, {c, "c"}} {
		child := orderedChild(t, root, dml.Source{DB: item.db}, ComponentBufferedImperative, "")
		orderedInsert(t, orderedData(t, child), 1, item.label)
		child.seal()
	}
	r.active = true
	if err := root.complete(t.Context(), nil); err == nil {
		t.Fatal("commit failure lost")
	}
	if got := physicalTerminal(r.copy(), "COMMIT"); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatal(got)
	}
	// B's driver rolls back after failed COMMIT; C is untouched until genuine
	// remaining forward cleanup. No second completion of B is attempted.
	if got := physicalTerminal(r.copy(), "ROLLBACK"); !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatal(got)
	}
	if orderedCount(t, a, "records") != 1 || orderedCount(t, b, "records") != 0 || orderedCount(t, c, "records") != 0 {
		t.Fatal("partial durability differs")
	}
	if root.units[0].data.(*dml.Data).TransactionOutcome().State != xh.TransactionCommitUnknown || root.units[1].data.(*dml.Data).TransactionOutcome().State != xh.TransactionRolledBack {
		t.Fatal("actual per-owner outcomes differ")
	}
}

func TestOrderedJournalReaderAndAllocationStart(t *testing.T) {
	for _, kind := range []string{"reader", "allocation", "only-one-start", "lazy"} {
		t.Run(kind, func(t *testing.T) {
			r := &orderedTrace{}
			a, b := orderedDB(t, r, "A", "", false), orderedDB(t, r, "B", "", false)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			if err := root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			ad := orderedData(t, root)
			orderedInsert(t, ad, 1, "a")
			child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			bd := orderedData(t, child)
			r.active = true
			switch kind {
			case "reader":
				var value int
				if err := bd.(*dml.Data).QueryRowContext(t.Context(), "SELECT 1").Scan(&value); err != nil || value != 1 {
					t.Fatalf("reader=%d err=%v", value, err)
				}
			case "allocation":
				if err := bd.Allocate(t.Context(), "records", &orderedRow{ID: 1}, "ID"); err != nil {
					t.Fatal(err)
				}
			case "only-one-start":
				if err := bd.(xh.TransactionStarter).Start(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "lazy" {
				for n := 0; n < 3; n++ {
					if err := bd.(xh.TransactionStarter).Start(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}
			orderedInsert(t, bd, 1, "b")
			child.seal()
			if len(physicalLabels(r.copy())) != 0 {
				t.Fatal("start/read/allocation drained product statements")
			}
			if err := root.complete(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			want := []string{"B", "A"}
			if kind == "lazy" {
				want = []string{"A", "B"}
			}
			if got := physicalTerminal(r.copy(), "COMMIT"); !reflect.DeepEqual(got, want) {
				t.Fatalf("commits=%v want=%v", got, want)
			}
			if got := physicalTerminal(r.copy(), "BEGIN"); !reflect.DeepEqual(got, want) {
				t.Fatalf("successful starts=%v want=%v", got, want)
			}
			if len(root.nativeInvocation.TransactionOwners()) != 2 {
				t.Fatal("repeated startup duplicated ordinal")
			}
			var infrastructure int
			if err := b.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='sqlx_sequence_reservations'").Scan(&infrastructure); err != nil {
				t.Fatal(err)
			}
			t.Logf("sqlite allocator infrastructure=%d", infrastructure)
		})
	}
}

type orderedCallbackRow struct {
	ID    int                         `sqlx:"ID,primaryKey"`
	Label string                      `sqlx:"LABEL"`
	run   func(context.Context) error `sqlx:"-"`
}

func (r *orderedCallbackRow) OnInsert(ctx context.Context) error {
	if r.run != nil {
		return r.run(ctx)
	}
	return nil
}
func TestOrderedJournalGuardsAndCallbackFailures(t *testing.T) {
	for _, mode := range []string{"payload-before", "payload-between", "denial", "panic", "cancellation", "guard-before", "on-commit-panic"} {
		t.Run(mode, func(t *testing.T) {
			trace := &orderedTrace{}
			a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
			source := dml.Source{DB: a}
			if mode == "on-commit-panic" {
				source.OnCommit = func(context.Context) { panic("ordered observer panic") }
			}
			root, _ := invocationDataScope(t.Context(), source)
			if err := root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			ad := orderedData(t, root)
			future := &orderedRow{ID: 1, Label: "future"}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			first := &orderedCallbackRow{ID: 1, Label: "first"}
			var ignored error
			first.run = func(callbackContext context.Context) error {
				switch mode {
				case "payload-between":
					future.Label = "changed"
				case "denial":
					ignored = root.data.(*dml.Data).PrepareCompletion(callbackContext)
				case "panic":
					panic("ordered insert panic")
				case "cancellation":
					cancel()
				}
				return nil
			}
			if err := ad.Insert("records", first); err != nil {
				t.Fatal(err)
			}
			child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			bd := orderedData(t, child)
			if err := bd.(interface {
				InsertWithQueueContract(string, any, rh.QueueContract) error
			}).InsertWithQueueContract("records", []*orderedRow{future}, rh.SourceSlice); err != nil {
				t.Fatal(err)
			}
			child.seal()
			if mode == "payload-before" {
				future.Label = "changed"
			}
			if mode == "guard-before" {
				if err := root.registerExecutionGuard(t.Context(), func(context.Context) error { return fmt.Errorf("ordered preflight failure") }); err != nil {
					t.Fatal(err)
				}
			}
			trace.active = true
			err := root.complete(ctx, nil)
			if err == nil {
				t.Fatal("failure was swallowed")
			}
			labels := physicalLabels(trace.copy())
			switch mode {
			case "payload-before", "guard-before", "panic":
				if len(labels) != 0 {
					t.Fatalf("unexpected prefix=%v", labels)
				}
			case "payload-between", "denial":
				if !reflect.DeepEqual(labels, []string{"A:first"}) {
					t.Fatalf("unexpected prefix=%v", labels)
				}
			case "on-commit-panic":
				if !reflect.DeepEqual(labels, []string{"A:first", "B:future"}) {
					t.Fatal(labels)
				}
			}
			if mode == "denial" && !errors.Is(ignored, drainowner.ErrDrain) {
				t.Fatalf("caught callback denial=%v", ignored)
			}
			if mode == "cancellation" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
			wantA := 0
			if mode == "on-commit-panic" {
				wantA = 1
				if root.data.(*dml.Data).TransactionOutcome().State != xh.TransactionCommitted {
					t.Fatal("observer error rewrote committed outcome")
				}
			}
			if n := orderedCount(t, a, "records"); n != wantA {
				t.Fatalf("A durable=%d want=%d", n, wantA)
			}
			if orderedCount(t, b, "records") != 0 {
				t.Fatal("B suffix durable")
			}
			commits := physicalTerminal(trace.copy(), "COMMIT")
			if mode == "on-commit-panic" {
				if !reflect.DeepEqual(commits, []string{"A"}) {
					t.Fatal(commits)
				}
			} else if len(commits) != 0 {
				t.Fatalf("failure committed %v", commits)
			}
		})
	}
}

type orderedAliasProvider map[string]*sql.DB

func (p orderedAliasProvider) Connector(_ context.Context, name string) (*sql.DB, error) {
	db := p[name]
	if db == nil {
		return nil, fmt.Errorf("unknown connector %s", name)
	}
	return db, nil
}
func (p orderedAliasProvider) ConnectorDataSource(ctx context.Context, name string) (dexec.DataSource, error) {
	db, err := p.Connector(ctx, name)
	if err != nil {
		return nil, err
	}
	return dml.Source{DB: db}, nil
}
func TestOrderedJournalConnectorAliasesShareOneOwner(t *testing.T) {
	trace := &orderedTrace{}
	a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
	root.connectors = orderedAliasProvider{"secondary": b, "secondary_alias": b}
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad := orderedData(t, root)
	orderedInsert(t, ad, 1, "before")
	trace.active = true
	for n, name := range []string{"secondary", "secondary_alias"} {
		service, err := (transactionSQLProvider{scope: root}).Connector(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		var value int
		if err = service.QueryRowContext(t.Context(), "SELECT 1").Scan(&value); err != nil || value != 1 {
			t.Fatalf("alias reader=%d err=%v", value, err)
		}
		source, err := root.connectors.(dexec.ConnectorDataSourceProvider).ConnectorDataSource(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		child, _ := invocationDataScope(PrepareComponent(withDataScope(t.Context(), root), ComponentBufferedImperative, ""), source)
		orderedInsert(t, orderedData(t, child), n+1, name)
		child.seal()
	}
	orderedInsert(t, ad, 2, "after")
	if len(root.units) != 1 {
		t.Fatalf("alias created %d foreign owners", len(root.units))
	}
	if err := root.complete(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"A:before", "B:secondary", "B:secondary_alias", "A:after"}) {
		t.Fatal(got)
	}
	if got := physicalTerminal(trace.copy(), "COMMIT"); !reflect.DeepEqual(got, []string{"B", "A"}) {
		t.Fatal(got)
	}
	if len(root.nativeInvocation.TransactionOwners()) != 2 {
		t.Fatal("alias duplicated transaction ordinal")
	}
}

type orderedPanicFinalizer struct {
	t     *testing.T
	trace *orderedTrace
	calls int
}

func (o *orderedPanicFinalizer) Finalize(_ context.Context, cause error) error {
	o.calls++
	if cause != nil {
		o.t.Fatalf("unexpected prepare failure=%v", cause)
	}
	if got := physicalLabels(o.trace.copy()); !reflect.DeepEqual(got, []string{"A:root", "B:child"}) {
		o.t.Fatalf("finalizer saw unprepared work %v", got)
	}
	if len(physicalTerminal(o.trace.copy(), "COMMIT")) != 0 {
		o.t.Fatal("finalizer ran after commit")
	}
	panic("ordered finalizer panic")
}
func TestOrderedJournalFinalizerPanicRollsBackPreparedOwners(t *testing.T) {
	trace := &orderedTrace{}
	a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
	out := &orderedPanicFinalizer{t: t, trace: trace}
	trace.active = true
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: a}, BufferedComponentCalls: true, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
		v, _, err := in.Binder.Lookup(ctx, xh.DataKey)
		if err != nil {
			return nil, err
		}
		if err = v.(xh.Data).Insert("records", &orderedRow{ID: 1, Label: "root"}); err != nil {
			return nil, err
		}
		_, err = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: b}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
			v, _, err := in.Binder.Lookup(ctx, xh.DataKey)
			if err != nil {
				return nil, err
			}
			return nil, v.(xh.Data).Insert("records", &orderedRow{ID: 1, Label: "child"})
		})})
		if err != nil {
			return nil, err
		}
		return out, nil
	})})
	var panicError *dexec.PanicError
	if !errors.As(err, &panicError) || out.calls != 1 {
		t.Fatalf("panic=%v calls=%d", err, out.calls)
	}
	if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"A:root", "B:child"}) {
		t.Fatal(got)
	}
	if len(physicalTerminal(trace.copy(), "COMMIT")) != 0 || orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
		t.Fatal("finalizer panic left durable work")
	}
	if got := physicalTerminal(trace.copy(), "ROLLBACK"); !reflect.DeepEqual(got, []string{"B", "A"}) {
		t.Fatal(got)
	}
}

func TestOrderedJournalEventSliceFailureStopsSuffix(t *testing.T) {
	trace := &orderedTrace{}
	a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
	if _, err := b.Exec("CREATE TRIGGER event_failure AFTER INSERT ON events WHEN NEW.LABEL='event2' BEGIN SELECT RAISE(ABORT,'event batch failure');END"); err != nil {
		t.Fatal(err)
	}
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	ad := orderedData(t, root)
	orderedInsert(t, ad, 1, "root")
	child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
	orderedInsert(t, orderedData(t, child), 1, "log")
	child.seal()
	events := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
	ed := orderedData(t, events)
	if err := ed.(interface {
		InsertWithQueueContract(string, any, rh.QueueContract) error
	}).InsertWithQueueContract("events", []*orderedRow{{ID: 1, Label: "event1"}, {ID: 2, Label: "event2"}}, rh.SourceSlice); err != nil {
		t.Fatal(err)
	}
	events.seal()
	orderedInsert(t, ad, 2, "forbidden-suffix")
	trace.active = true
	if err := root.complete(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "event batch failure") {
		t.Fatalf("event failure=%v", err)
	}
	if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"A:root", "B:log"}) {
		t.Fatal(got)
	}
	eventStatements := 0
	for _, line := range trace.copy() {
		if strings.HasPrefix(line, "B INSERT INTO events") {
			eventStatements++
			if !strings.Contains(line, "event1") || !strings.Contains(line, "event2") {
				t.Fatalf("generic slice shape=%s", line)
			}
		}
	}
	if eventStatements != 1 {
		t.Fatalf("generic slice statements=%d want=1", eventStatements)
	}
	if len(physicalTerminal(trace.copy(), "COMMIT")) != 0 || !reflect.DeepEqual(physicalTerminal(trace.copy(), "ROLLBACK"), []string{"B", "A"}) {
		t.Fatal(trace.copy())
	}
	if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 || orderedCount(t, b, "events") != 0 {
		t.Fatal("event failure left durable work")
	}
	t.Log(strings.Join(trace.copy(), "\n"))
}
