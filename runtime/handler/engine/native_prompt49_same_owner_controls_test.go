package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

func prompt49Call(name string, ctx context.Context, d *dml.Data) error {
	switch name {
	case "Start":
		return d.Start(ctx)
	case "Reserve":
		return d.Reserve(ctx, "records", &terminal49Row{ID: 123, Label: "supplied"}, "ID")
	case "Allocate":
		return d.Allocate(ctx, "records", &terminal49Row{Label: "allocated"}, "ID")
	case "ExecContext":
		_, e := d.ExecContext(ctx, "INSERT INTO records VALUES(2,'late')")
		return e
	case "QueryContext":
		rows, e := d.QueryContext(ctx, "INSERT INTO records VALUES(2,'late') RETURNING id")
		if rows != nil {
			rows.Close()
		}
		return e
	case "QueryRowContext":
		var id int
		return d.QueryRowContext(ctx, "INSERT INTO records VALUES(2,'late') RETURNING id").Scan(&id)
	case "RegisterExecutionGuard":
		return d.RegisterExecutionGuard(func(context.Context) error { return nil })
	case "EnableCapturedExecutionGuards":
		return d.EnableCapturedExecutionGuards()
	case "BeginInvocation":
		return d.BeginInvocation()
	case "ValidateExecutionGuards":
		return d.ValidateExecutionGuards(ctx)
	case "CloseMutationAdmission":
		return d.CloseMutationAdmission()
	}
	return fmt.Errorf("unknown native method %s", name)
}
func prompt49Open(t *testing.T, path string, setup bool) *sql.DB {
	t.Helper()
	db, e := sql.Open("sqlite3", path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	if setup {
		for _, q := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value>=0))"} {
			if _, e = db.Exec(q); e != nil {
				t.Fatal(e)
			}
		}
	}
	return db
}
func prompt49Counts(t *testing.T, db *sql.DB) (int, int) {
	t.Helper()
	var rows, audit int
	if e := db.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); e != nil {
		t.Fatal(e)
	}
	if e := db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audit); e != nil {
		t.Fatal(e)
	}
	return rows, audit
}

// A subprocess bounds an actual same-owner lock wait without leaving blocked
// callbacks in the test process or pretending that an uncertain outcome returned.
func TestNativePrompt49DesiredSameOwnerCommitCallback(t *testing.T) {
	for _, only := range []bool{true, false} {
		for _, op := range []string{"Start", "Reserve", "Allocate", "ExecContext", "QueryContext", "QueryRowContext", "RegisterExecutionGuard", "EnableCapturedExecutionGuards", "BeginInvocation", "ValidateExecutionGuards", "CloseMutationAdmission"} {
			t.Run(fmt.Sprintf("%s/only=%t", op, only), func(t *testing.T) {
				dir := t.TempDir()
				if root := os.Getenv("NATIVE49_EVIDENCE_DIR"); root != "" {
					var e error
					dir, e = os.MkdirTemp(root, "same-owner-")
					if e != nil {
						t.Fatal(e)
					}
				}
				exe, e := os.Executable()
				if e != nil {
					t.Fatal(e)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, exe, "-test.run=^TestNativePrompt49Child$", "-test.v", "-test.timeout=5s")
				cmd.Env = append(os.Environ(), "NATIVE49_PROMPT_CHILD=1", "NATIVE49_PROMPT_OP="+op, fmt.Sprintf("NATIVE49_PROMPT_ONLY=%t", only), "NATIVE49_PROMPT_DIR="+dir)
				raw, childErr := cmd.CombinedOutput()
				if e = os.WriteFile(filepath.Join(dir, "child.log"), raw, 0600); e != nil {
					t.Fatal(e)
				}
				t.Logf("ACTUAL_CALLBACK_CHILD op=%s only=%t error=%v deadline=%v dir=%s\n%s", op, only, childErr, ctx.Err(), dir, raw)
				if !strings.Contains(string(raw), "ACTUAL_COMMIT_CALLBACK_ENTERED rows=1 audit=1") {
					t.Fatal("intended actual committed callback stage not reached")
				}
				for _, name := range []string{"observer.db", "first.db"} {
					if only && name == "first.db" {
						continue
					}
					db := prompt49Open(t, filepath.Join(dir, name), false)
					rows, audit := prompt49Counts(t, db)
					if rows != 1 || audit != 1 {
						t.Fatalf("truthful physical commit changed %s rows=%d audit=%d", name, rows, audit)
					}
					t.Logf("DURABLE_PHYSICAL_COMMIT %s rows=%d audit=%d", name, rows, audit)
				}
				if childErr != nil {
					t.Fatalf("DESIRED_PROMPT_NATIVE_GAP same-owner %s callback failed to return promptly: %v", op, childErr)
				}
				if !strings.Contains(string(raw), "ACTUAL_COMMIT_CALLBACK_RETURNED outcome=committed") {
					t.Fatal("successful child did not prove actual native Committed outcome")
				}
			})
		}
	}
}
func TestNativePrompt49Child(t *testing.T) {
	if os.Getenv("NATIVE49_PROMPT_CHILD") != "1" {
		t.Skip("subprocess only")
	}
	ctx := t.Context()
	dir := os.Getenv("NATIVE49_PROMPT_DIR")
	only := os.Getenv("NATIVE49_PROMPT_ONLY") == "true"
	op := os.Getenv("NATIVE49_PROMPT_OP")
	observerDB := prompt49Open(t, filepath.Join(dir, "observer.db"), true)
	var firstDB *sql.DB
	if !only {
		firstDB = prompt49Open(t, filepath.Join(dir, "first.db"), true)
	}
	var observer *dataScope
	var attempt error
	callbacks := 0
	source := dml.Source{DB: observerDB, OnCommit: func(ctx context.Context) {
		callbacks++
		if observer == nil {
			t.Fatal("missing genuine observer unit")
		}
		rows, audit := prompt49Counts(t, observerDB)
		fmt.Printf("ACTUAL_COMMIT_CALLBACK_ENTERED rows=%d audit=%d op=%s\n", rows, audit, op)
		attempt = prompt49Call(op, ctx, observer.data.(*dml.Data))
		fmt.Printf("ACTUAL_CALLBACK_METHOD_RETURNED error=%v\n", attempt)
	}}
	firstSource := source
	if !only {
		firstSource = dml.Source{DB: firstDB}
	}
	result, e := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: firstSource, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
		value, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
		if e != nil || !ok {
			return nil, fmt.Errorf("real data lookup found=%t: %w", ok, e)
		}
		if e = value.(xh.Data).Execute("INSERT INTO records VALUES(1,'admitted')"); e != nil {
			return nil, e
		}
		if only {
			observer = mainScope(ctx)
			return "ordinary successful result", nil
		}
		_, e = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
			v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
			if e != nil || !ok {
				return nil, e
			}
			observer = mainScope(ctx).unit
			return nil, v.(xh.Data).Execute("INSERT INTO records VALUES(1,'admitted')")
		})})
		return "ordinary successful result", e
	})})
	if callbacks != 1 || attempt == nil || e == nil || result != nil {
		t.Fatalf("ignored actual callback rejection lost callbacks=%d attempt=%v root=%v result=%v", callbacks, attempt, e, result)
	}
	if !errors.Is(attempt, drainowner.ErrDrain) && !errors.Is(attempt, dml.ErrMutationAdmissionClosed) {
		t.Fatalf("unexpected prompt admission result=%v", attempt)
	}
	native := observer.data.(*dml.Data)
	out := native.TransactionOutcome()
	if out.State != xh.TransactionCommitted || out.Error != nil || observer.completionErr != nil {
		t.Fatalf("truthful committed outcome rewritten=%+v completion=%v", out, observer.completionErr)
	}
	_, tx := native.InvocationTransaction()
	if tx == nil {
		t.Fatal("missing committed TX")
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO records VALUES(101,'retry')"); !errors.Is(e, sql.ErrTxDone) {
		t.Fatalf("committed TX reopened: %v", e)
	}
	fmt.Printf("ACTUAL_COMMIT_CALLBACK_RETURNED outcome=%s root=%v\n", out.State, e)
}
