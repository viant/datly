package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type commandHas struct{ ID, Name, Owner, Amount bool }
type commandRow struct {
	ID     int         `sqlx:"id,primaryKey=true,autoincrement=true"`
	Name   string      `sqlx:"name" validate:"required"`
	Owner  *string     `sqlx:"owner,refTable=owners,refColumn=name"`
	Amount int         `sqlx:"amount"`
	Has    *commandHas `setMarker:"true" sqlx:"-"`
}
type commandInput struct {
	Rows []*commandRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
}
type commandOutput struct {
	Data []*commandRow `parameter:"Data,kind=output,in=body"`
}

func commandComponent(policy, operation string) *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[commandRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: operation}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: policy, Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, AutoIncrement: true}, {Name: "name", Source: "name"}, {Name: "owner", Source: "owner"}, {Name: "amount", Source: "amount"}}}}
}

func TestUpdateCommandSharedNativeJournalSQLite(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint("lateFailure=", late), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, `PRAGMA foreign_keys=ON;CREATE TABLE owners(name TEXT PRIMARY KEY);INSERT INTO owners VALUES('old'),('live');CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,owner TEXT REFERENCES owners(name),amount INTEGER NOT NULL);CREATE TABLE audit(name TEXT,amount INTEGER,owner TEXT);CREATE TRIGGER updated AFTER UPDATE ON records BEGIN INSERT INTO audit VALUES(NEW.name,NEW.amount,NEW.owner);END;CREATE TABLE dependents(id INTEGER PRIMARY KEY,record_id INTEGER REFERENCES records(id))`); err != nil {
				t.Fatal(err)
			}
			commits := 0
			source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
			insert, err := New(commandComponent("", "post"), reflect.TypeFor[commandInput](), reflect.TypeFor[commandOutput](), "post")
			if err != nil {
				t.Fatal(err)
			}
			update, err := New(commandComponent("update", "patch"), reflect.TypeFor[commandInput](), reflect.TypeFor[commandOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			if update.metadata.CurrentField != -1 || update.metadata.Sequence != nil {
				t.Fatal("command has Current or sequence")
			}
			initial := &commandRow{Name: "inserted", Owner: ptr("old"), Amount: 5, Has: &commandHas{Name: true, Owner: true, Amount: true}}
			invoke := func(ctx context.Context, h *Handler, row *commandRow) (any, error) {
				return engine.New().Execute(engine.PrepareComponent(ctx, engine.ComponentBufferedImperative, ""), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[commandInput]()), BoundInput: &commandInput{Rows: []*commandRow{row}}, Handler: h, DataSource: source})
			}
			queued := 0
			request := engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, DataSource: source, BufferedComponentCalls: true, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				if _, err := invoke(ctx, insert, initial); err != nil {
					return nil, err
				}
				if initial.ID == 0 {
					return nil, errors.New("native POST did not allocate")
				}
				// Source captures the scalar struct at each update point. Has and pointer
				// fields deliberately remain shared, matching the existing SQLX contract.
				input := &commandRow{ID: initial.ID, Name: "first", Owner: ptr("old"), Amount: 6, Has: &commandHas{Name: true, Owner: true}}
				first := *input
				if _, err := invoke(ctx, update, &first); err != nil {
					return nil, err
				}
				queued++
				input.Name = "second"
				second := *input
				if _, err := invoke(ctx, update, &second); err != nil {
					return nil, err
				}
				queued++
				third := *input
				if _, err := invoke(ctx, update, &third); err != nil {
					return nil, err
				}
				queued++
				// Live presence and pointer values are not frozen by command admission.
				input.Has.Amount = true
				*input.Owner = "live"
				value, found, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
				if err != nil || !found {
					return nil, fmt.Errorf("DML lookup: %v", err)
				}
				native := value.(xhandler.DML)
				if err = native.Insert("dependents", &commandDependent{ID: 1, RecordID: initial.ID}); err != nil {
					return nil, err
				}
				if late {
					bad := commandRow{ID: initial.ID, Owner: ptr("missing"), Has: &commandHas{Owner: true}}
					if _, err = invoke(ctx, update, &bad); err != nil {
						return nil, fmt.Errorf("premature command validation: %w", err)
					}
				}
				if commits != 0 {
					return nil, errors.New("child committed")
				}
				tx, err := dexec.InvocationTransaction(ctx, db.DB)
				if err != nil || tx == nil {
					return nil, fmt.Errorf("shared transaction: %v", err)
				}
				var pending int
				if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&pending); err != nil || pending != 0 {
					return nil, fmt.Errorf("early drain count=%d: %v", pending, err)
				}
				return nil, nil
			})}
			_, err = engine.New().Execute(ctx, request)
			if late {
				var validation *xhandler.Validation
				if err == nil || !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") || errors.As(err, &validation) {
					t.Fatalf("expected database drain failure: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var count int
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if late {
				if count != 0 || commits != 0 {
					t.Fatalf("rollback count=%d commits=%d", count, commits)
				}
				return
			}
			if count != 1 || commits != 1 || queued != 3 {
				t.Fatalf("count=%d commits=%d queued=%d", count, commits, queued)
			}
			rows, err := db.DB.Query("SELECT name,amount,owner FROM audit ORDER BY rowid")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for _, want := range []string{"first", "second", "second"} {
				var name, owner string
				var amount int
				if !rows.Next() {
					t.Fatal("missing physical UPDATE trigger")
				}
				if err = rows.Scan(&name, &amount, &owner); err != nil || name != want || amount != 6 || owner != "live" {
					t.Fatalf("capture/order %q %d want=%q/6 err=%v", name, amount, want, err)
				}
			}
			if rows.Next() {
				t.Fatal("extra physical update")
			}
		})
	}
}

type commandDependent struct {
	ID       int `sqlx:"id,primaryKey=true"`
	RecordID int `sqlx:"record_id,refTable=records,refColumn=id"`
}

func TestUpdateCommandZeroKeyWithoutIdentityPresenceSQLite(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL,owner TEXT,amount INTEGER NOT NULL);INSERT INTO records VALUES(0,'keep',NULL,7)`); err != nil {
		t.Fatal(err)
	}
	row := &commandRow{ID: 0, Name: "changed", Has: &commandHas{Name: true}}
	result, err := runSQLiteWriter(t, ctx, db, commandComponent("update", "patch"), &commandInput{Rows: []*commandRow{row}}, &commandOutput{}, "patch")
	if err != nil {
		t.Fatal(err)
	}
	if row.ID != 0 || row.Has.ID || result.(*commandOutput).Data[0] != row {
		t.Fatal("command normalized identity or output")
	}
	var name string
	var amount int
	if err = db.DB.QueryRow("SELECT name,amount FROM records WHERE id=0").Scan(&name, &amount); err != nil || name != "changed" || amount != 7 {
		t.Fatalf("zero sparse update: %q/%d %v", name, amount, err)
	}
}

// An identity-only command still enters the ordinary typed journal. SQLX owns
// empty SET behavior, including a shared marker becoming live before drain.
func TestUpdateCommandEmptySetMatchesSQLXSQLite(t *testing.T) {
	for _, gain := range []bool{false, true} {
		t.Run(fmt.Sprint("laterPresence=", gain), func(t *testing.T) {
			results := make([]string, 2)
			for i, native := range []bool{false, true} {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL,owner TEXT,amount INTEGER NOT NULL);INSERT INTO records VALUES(0,'keep',NULL,7)`); err != nil {
					t.Fatal(err)
				}
				data := dml.NewData(db.DB)
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				row := &commandRow{ID: 0, Name: "live", Has: &commandHas{}}
				var err error
				if native {
					h, e := New(commandComponent("update", "patch"), reflect.TypeFor[commandInput](), reflect.TypeFor[commandOutput](), "patch")
					if e != nil {
						t.Fatal(e)
					}
					input := &commandInput{Rows: []*commandRow{row}}
					snapshot, e := h.CaptureInput(ctx, input)
					if e != nil {
						t.Fatal(e)
					}
					_, err = h.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}})
				} else {
					err = data.Update("records", row)
				}
				if err != nil {
					t.Fatal(err)
				}
				if gain {
					row.Has.Name = true
				}
				err = data.Complete(ctx, nil)
				if err != nil {
					results[i] = "error:" + err.Error()
				} else if err = db.DB.QueryRow("SELECT name FROM records WHERE id=0").Scan(&results[i]); err != nil {
					t.Fatal(err)
				}
			}
			if results[0] != results[1] {
				t.Fatalf("typed SQLX/native diverged: %q", results)
			}
			if gain && results[1] != "live" {
				t.Fatalf("live Has was frozen: %q", results)
			}
		})
	}
}

type commandHookKey struct{}
type commandHookProbe struct{ init, validate, finalize int }
type commandHooks struct{}

var commandHookLinked = reflect.TypeFor[commandHooks]()

func (*commandHooks) Init(ctx context.Context, row *commandRow, state xhandler.LifecycleContext[commandRow, xhandler.NoParent, commandOutput]) error {
	p := ctx.Value(commandHookKey{}).(*commandHookProbe)
	p.init++
	if state.Previous != nil {
		return errors.New("fabricated Previous")
	}
	return nil
}
func (*commandHooks) Validate(ctx context.Context, row *commandRow, state xhandler.LifecycleContext[commandRow, xhandler.NoParent, commandOutput]) error {
	p := ctx.Value(commandHookKey{}).(*commandHookProbe)
	p.validate++
	if state.Previous != nil {
		return errors.New("fabricated Previous")
	}
	return nil
}
func (*commandHooks) Finalize(ctx context.Context, _ *commandInput, _ *commandOutput, _ xhandler.Outcome) error {
	ctx.Value(commandHookKey{}).(*commandHookProbe).finalize++
	return nil
}

type commandNoValidation struct {
	*sqBinder
	calls int
}

func (b *commandNoValidation) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.FrameworkValidatorKey {
		return b, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}
func (b *commandNoValidation) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	b.calls++
	return nil, errors.New("command invoked hydrated framework validation")
}
func TestUpdateCommandBusinessHooksWithoutFrameworkValidationSQLite(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL,owner TEXT,amount INTEGER NOT NULL);INSERT INTO records VALUES(0,'keep',NULL,7)`); err != nil {
		t.Fatal(err)
	}
	c := commandComponent("update", "patch")
	c.RootView.EntityHooks = commandHookLinked.Name()
	h, err := New(c, reflect.TypeFor[commandInput](), reflect.TypeFor[commandOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	data := dml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	probe := &commandHookProbe{}
	ctx = context.WithValue(ctx, commandHookKey{}, probe)
	binder := &commandNoValidation{sqBinder: &sqBinder{data: data}}
	input := &commandInput{Rows: []*commandRow{{ID: 0, Amount: 0, Has: &commandHas{Amount: true}}}}
	snapshot, err := h.CaptureInput(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	inv := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: binder}
	result, err := h.Execute(ctx, inv)
	if err = data.Complete(ctx, err); err != nil {
		t.Fatal(err)
	}
	if err = h.FinalizeOutcome(ctx, inv, result, xhandler.Outcome{}); err != nil {
		t.Fatal(err)
	}
	if binder.calls != 0 || probe.init != 1 || probe.validate != 1 || probe.finalize != 1 {
		t.Fatalf("framework=%d business=%+v", binder.calls, probe)
	}
}

type commandCompositeHas struct{ Tenant, ID, Name bool }
type commandCompositeRow struct {
	Tenant int                  `sqlx:"tenant,primaryKey=true"`
	ID     int                  `sqlx:"id,primaryKey=true"`
	Name   string               `sqlx:"name"`
	Has    *commandCompositeHas `setMarker:"true" sqlx:"-"`
}
type commandCompositeInput struct {
	Rows []*commandCompositeRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
}
type commandCompositeOutput struct {
	Data []*commandCompositeRow `parameter:"Data,kind=output,in=body"`
}

func TestUpdateCommandCompositeZeroKeysSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE records(tenant INTEGER,id INTEGER,name TEXT,PRIMARY KEY(tenant,id));INSERT INTO records VALUES(0,0,'target'),(1,0,'other')`); err != nil {
		t.Fatal(err)
	}
	c := commandComponent("update", "patch")
	c.RootView.Columns = []*spec.Column{{Name: "tenant", Source: "tenant", PrimaryKey: true}, {Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}
	row := &commandCompositeRow{Name: "changed", Has: &commandCompositeHas{Name: true}}
	if _, err := runSQLiteWriter(t, ctx, db, c, &commandCompositeInput{Rows: []*commandCompositeRow{row}}, &commandCompositeOutput{}, "patch"); err != nil {
		t.Fatal(err)
	}
	var actual string
	if err := db.DB.QueryRow("SELECT name FROM records WHERE tenant=0 AND id=0").Scan(&actual); err != nil || actual != "changed" {
		t.Fatalf("zero composite: %q %v", actual, err)
	}
	if err := db.DB.QueryRow("SELECT name FROM records WHERE tenant=1 AND id=0").Scan(&actual); err != nil || actual != "other" {
		t.Fatalf("composite criteria: %q %v", actual, err)
	}
	if row.Has.ID || row.Has.Tenant {
		t.Fatal("command normalized key presence")
	}
}
