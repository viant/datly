package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type protectedEngineKey struct{}
type protectedEngineProbe struct {
	db                        *sql.DB
	tx                        *sql.Tx
	explicit, shortCaller     bool
	batchCalls, commits       int
	before, selected, sibling [3]int
	callerCanceled            bool
}

type protectedEngineHooks struct {
	Flush xhandler.Flusher `bind:"kind=flusher,required"`
	DML   xhandler.DML     `bind:"kind=dml,required"`
}

var protectedEngineLinked = reflect.TypeFor[protectedEngineHooks]()

func (hook *protectedEngineHooks) AfterQueueInput(ctx context.Context, _ *sqPlainInput, _ *sqPlainOutput) error {
	p := ctx.Value(protectedEngineKey{}).(*protectedEngineProbe)
	p.batchCalls++
	if _, exposed := hook.Flush.(xhandler.Data); exposed {
		return errors.New("lifecycle flusher exposed data lifecycle")
	}
	if p.explicit {
		// An authorized no-match target must leave the native graph queued.
		if err := hook.Flush.Flush(ctx, "unused"); err != nil {
			return err
		}
		var err error
		if p.before, err = protectedEngineCounts(ctx, p); err != nil {
			return err
		}
		call := ctx
		cancel := func() {}
		if p.shortCaller {
			call, cancel = context.WithCancel(ctx)
		}
		err = hook.Flush.Flush(call, "children")
		cancel()
		if err != nil {
			return err
		}
		if p.shortCaller {
			p.callerCanceled = errors.Is(call.Err(), context.Canceled)
		}
		// Repeating the boundary must not replay the inserts or audit triggers.
		if err = hook.Flush.Flush(ctx, "children"); err != nil {
			return err
		}
	}
	if err := hook.DML.Execute("INSERT INTO suffix(id) VALUES(1)"); err != nil {
		return err
	}
	var err error
	p.selected, err = protectedEngineCounts(ctx, p)
	return err
}

func protectedEngineCounts(ctx context.Context, p *protectedEngineProbe) ([3]int, error) {
	var result [3]int
	tx, err := dexec.InvocationTransaction(ctx, p.db)
	if err != nil {
		return result, err
	}
	if tx == nil {
		return result, errors.New("native invocation has no root transaction")
	}
	if p.tx != nil && tx != p.tx {
		return result, errors.New("sibling changed root transaction")
	}
	p.tx = tx
	for i, table := range []string{"parents", "children", "suffix"} {
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&result[i]); err != nil {
			return result, err
		}
	}
	return result, nil
}

func protectedEngineRoute(t *testing.T, typ reflect.Type) *registry.RouteInputContract {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(typ)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "PATCH", Path: "/protected-records"}
	contract, err := registry.NewInputContract(typ, projection, registry.RouteInput{Route: ref, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := contract.ForRoute(ref)
	if !ok {
		t.Fatal("missing protected writer route")
	}
	return route
}

func protectedEngineNative(t *testing.T, grant bool) (*Handler, *sqPlainInput) {
	t.Helper()
	component := sqComponent(protectedEngineLinked.PkgPath(), "patch", protectedEngineLinked.Name())
	if grant {
		component.Settings.ProtectedFlushTables = []string{"children", "unused"}
	}
	native, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	return native, &sqPlainInput{Rows: []*sqPlainParent{{
		ID: ptr(1), Name: ptr("parent"), Has: &sqPlainParentHas{ID: true, Name: true, Children: true},
		Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("child"), Has: &sqPlainChildHas{ID: true, Label: true}}},
	}}}
}

func protectedEngineDatabase(t *testing.T) *sqlite.Harness {
	t.Helper()
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(),
		"CREATE TABLE parents(id INTEGER PRIMARY KEY,name TEXT)",
		"CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parents(id),label TEXT)",
		"CREATE TABLE suffix(id INTEGER PRIMARY KEY)",
		"CREATE TABLE effects(kind TEXT)",
		"CREATE TRIGGER parent_effect AFTER INSERT ON parents BEGIN INSERT INTO effects VALUES('parent'); END",
		"CREATE TRIGGER child_effect AFTER INSERT ON children BEGIN INSERT INTO effects VALUES('child'); END",
		"CREATE TRIGGER suffix_effect AFTER INSERT ON suffix BEGIN INSERT INTO effects VALUES('suffix'); END",
	); err != nil {
		t.Fatal(err)
	}
	return db
}

func protectedEngineStored(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestProtectedFlushNativeEnginePaths(t *testing.T) {
	for _, test := range []struct {
		name                                        string
		relation                                    engine.ComponentRelation
		rootNative, neutral, explicit, cancel       bool
		grant, buffered, lateFailure, defaultDenied bool
	}{
		{name: "root lifecycle", rootNative: true, explicit: true, grant: true, buffered: true},
		{name: "imperative lifecycle and sibling", relation: engine.ComponentImperative, explicit: true, grant: true, buffered: true},
		{name: "buffered lifecycle and sibling", relation: engine.ComponentBufferedImperative, explicit: true, grant: true, buffered: true},
		{name: "source-less buffered parent", relation: engine.ComponentBufferedImperative, neutral: true, explicit: true, grant: true, buffered: true},
		{name: "configured declaration without call", relation: engine.ComponentImperative, grant: true, buffered: true},
		{name: "unconfigured batch hook retains guard rejection", relation: engine.ComponentImperative, defaultDenied: true},
		{name: "caller cancellation retains root", relation: engine.ComponentImperative, explicit: true, cancel: true, grant: true, buffered: true},
		{name: "late failure rolls back prefix and suffix", relation: engine.ComponentImperative, explicit: true, grant: true, buffered: true, lateFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := protectedEngineDatabase(t)
			p := &protectedEngineProbe{db: db.DB, explicit: test.explicit, shortCaller: test.cancel}
			ctx := context.WithValue(context.Background(), protectedEngineKey{}, p)
			source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { p.commits++ }}
			native, input := protectedEngineNative(t, test.grant)
			child := engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[sqPlainInput]()), BoundInput: input, Handler: native, DataSource: source}
			if test.grant {
				child.ProtectedFlushTables = []string{"children", "unused"}
			}
			request := child
			request.BufferedComponentCalls = test.buffered
			if !test.rootNative {
				request = engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, BufferedComponentCalls: test.buffered}
				if !test.neutral {
					request.DataSource = source
				}
				request.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					output, err := engine.New().Execute(engine.PrepareComponent(ctx, test.relation, ""), child)
					if err != nil {
						return output, err
					}
					_, err = engine.New().Execute(engine.PrepareComponent(ctx, engine.ComponentImperative, ""), engine.Request{
						Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, DataSource: source,
						Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
							var err error
							p.sibling, err = protectedEngineCounts(ctx, p)
							return nil, err
						}),
					})
					if err != nil {
						return nil, err
					}
					if p.commits != 0 || protectedEngineStored(t, db.DB, "parents") != 0 {
						return nil, errors.New("child committed before root completion")
					}
					if test.lateFailure {
						value, found, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
						if err != nil || !found {
							return nil, fmt.Errorf("root DML lookup: found=%v error=%v", found, err)
						}
						// The lifecycle suffix inserts id=1; this later duplicate fails
						// only after root completion physically executes that suffix.
						err = value.(xhandler.DML).Execute("INSERT INTO suffix(id) VALUES(1)")
						if err != nil {
							return nil, err
						}
					}
					return output, nil
				})
			}
			_, err := engine.New().Execute(ctx, request)
			if test.defaultDenied {
				if err == nil || !strings.Contains(err.Error(), "flush imperative writer: public native drain is forbidden in a protected invocation") {
					t.Fatalf("unconfigured writer automatic flush guard changed: %v", err)
				}
			} else if test.lateFailure {
				if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed: suffix.id") {
					t.Fatalf("late SQLite duplicate failure lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if p.batchCalls != 1 {
				t.Fatalf("AfterQueueInput calls=%d", p.batchCalls)
			}
			if test.explicit && p.before != [3]int{} {
				t.Fatalf("no-match flushed native rows: %v", p.before)
			}
			wantSelected := [3]int{}
			if test.explicit {
				wantSelected = [3]int{1, 1, 0}
			}
			if p.selected != wantSelected {
				t.Fatalf("lifecycle visibility=%v want=%v", p.selected, wantSelected)
			}
			if !test.rootNative && !test.defaultDenied {
				wantSibling := wantSelected
				if !test.grant {
					wantSibling = [3]int{1, 1, 1}
				}
				if p.sibling != wantSibling {
					t.Fatalf("sibling visibility=%v want=%v", p.sibling, wantSibling)
				}
			}
			if test.cancel && !p.callerCanceled {
				t.Fatal("short flush context was not canceled")
			}
			wantRows, wantEffects, wantCommits := 1, 3, 1
			if test.lateFailure || test.defaultDenied {
				wantRows, wantEffects, wantCommits = 0, 0, 0
			}
			for _, table := range []string{"parents", "children", "suffix"} {
				if got := protectedEngineStored(t, db.DB, table); got != wantRows {
					t.Fatalf("stored %s=%d want=%d", table, got, wantRows)
				}
			}
			if got := protectedEngineStored(t, db.DB, "effects"); got != wantEffects || p.commits != wantCommits {
				t.Fatalf("physical trigger effects=%d commits=%d want=%d/%d", got, p.commits, wantEffects, wantCommits)
			}
			if p.tx == nil {
				t.Fatal("root transaction never observed")
			}
			if _, err := p.tx.Exec("SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("root completion left transaction live: %v", err)
			}
		})
	}
}

func TestProtectedFlushNativeEngineHooklessDefault(t *testing.T) {
	db := protectedEngineDatabase(t)
	p := &protectedEngineProbe{db: db.DB}
	ctx := context.WithValue(context.Background(), protectedEngineKey{}, p)
	source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { p.commits++ }}
	_, input := protectedEngineNative(t, false)
	// With no retained batch callback the ordinary native writer has no
	// captured callback guard, and keeps its existing automatic empty flush.
	native, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.New().Execute(ctx, engine.Request{
		Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, DataSource: source,
		Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			_, err := engine.New().Execute(engine.PrepareComponent(ctx, engine.ComponentImperative, ""), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[sqPlainInput]()), BoundInput: input, DataSource: source, Handler: native,
			})
			if err != nil {
				return nil, err
			}
			_, err = engine.New().Execute(engine.PrepareComponent(ctx, engine.ComponentImperative, ""), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, DataSource: source,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
					var err error
					p.sibling, err = protectedEngineCounts(ctx, p)
					return nil, err
				}),
			})
			if err != nil {
				return nil, err
			}
			if p.sibling != [3]int{1, 1, 0} || p.commits != 0 || protectedEngineStored(t, db.DB, "parents") != 0 {
				return nil, fmt.Errorf("hookless automatic flush visibility=%v commits=%d", p.sibling, p.commits)
			}
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.commits != 1 || protectedEngineStored(t, db.DB, "parents") != 1 || protectedEngineStored(t, db.DB, "children") != 1 || protectedEngineStored(t, db.DB, "effects") != 2 {
		t.Fatal("hookless automatic flush did not commit exactly once at root completion")
	}
	if _, err := p.tx.Exec("SELECT 1"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("hookless root completion left transaction live: %v", err)
	}
}

func TestProtectedFlushNativeEngineGrantIsComponentLocal(t *testing.T) {
	db := protectedEngineDatabase(t)
	p := &protectedEngineProbe{db: db.DB, explicit: true}
	native, input := protectedEngineNative(t, false)
	ctx := context.WithValue(context.Background(), protectedEngineKey{}, p)
	source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { p.commits++ }}
	_, err := engine.New().Execute(ctx, engine.Request{
		Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, DataSource: source,
		BufferedComponentCalls: true, ProtectedFlushTables: []string{"children", "unused"},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			return engine.New().Execute(engine.PrepareComponent(ctx, engine.ComponentBufferedImperative, ""), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[sqPlainInput]()), BoundInput: input, Handler: native, DataSource: source,
			})
		}),
	})
	if err == nil || p.batchCalls != 1 {
		t.Fatalf("ungranted child lifecycle inherited grant: calls=%d error=%v", p.batchCalls, err)
	}
	for _, table := range []string{"parents", "children", "suffix", "effects"} {
		if got := protectedEngineStored(t, db.DB, table); got != 0 {
			t.Fatalf("ungranted child wrote %s=%d", table, got)
		}
	}
	if p.commits != 0 {
		t.Fatal("failed ungranted child committed")
	}
}
