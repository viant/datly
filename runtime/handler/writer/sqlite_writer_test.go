package writer

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"

	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

// --- fixture entities -------------------------------------------------------

type sqParentHas struct{ ID, Name, Version, Remove, Children bool }
type sqChildHas struct{ ID, ParentID, Label, Version, Remove bool }

type sqParent struct {
	ID       *int         `sqlx:"id,primaryKey=true"`
	Name     *string      `sqlx:"name"`
	Version  *int         `sqlx:"version" writer:"concurrency"`
	Remove   bool         `sqlx:"-" writer:"delete"`
	Children []*sqChild   `view:"Children,table=children" on:"ID=ParentID"`
	Has      *sqParentHas `setMarker:"true" sqlx:"-" json:"-"`
}

type sqChild struct {
	ID       *int        `sqlx:"id,primaryKey=true"`
	ParentID *int        `sqlx:"parent_id"`
	Label    *string     `sqlx:"label"`
	Version  *int        `sqlx:"version" writer:"concurrency"`
	Remove   bool        `sqlx:"-" writer:"delete"`
	Has      *sqChildHas `setMarker:"true" sqlx:"-" json:"-"`
}

// sqPlainChild has no concurrency token.
type sqPlainChildHas struct{ ID, ParentID, Label, Remove bool }
type sqPlainChild struct {
	ID       *int             `sqlx:"id,primaryKey=true"`
	ParentID *int             `sqlx:"parent_id"`
	Label    *string          `sqlx:"label"`
	Remove   bool             `sqlx:"-" writer:"delete"`
	Has      *sqPlainChildHas `setMarker:"true" sqlx:"-" json:"-"`
}
type sqPlainParentHas struct{ ID, Name, Remove, Children bool }
type sqPlainParent struct {
	ID       *int              `sqlx:"id,primaryKey=true"`
	Name     *string           `sqlx:"name"`
	Remove   bool              `sqlx:"-" writer:"delete"`
	Children []*sqPlainChild   `view:"Children,table=children" on:"ID=ParentID"`
	Has      *sqPlainParentHas `setMarker:"true" sqlx:"-" json:"-"`
}

type sqInput struct {
	Rows            []*sqParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*sqParent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*sqChild  `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}
type sqOutput struct {
	Data   []*sqParent `parameter:"Data,kind=output,in=body"`
	Events []string
	Status string
}

type sqPlainInput struct {
	Rows            []*sqPlainParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*sqPlainParent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*sqPlainChild  `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}
type sqPlainOutput struct {
	Data   []*sqPlainParent `parameter:"Data,kind=output,in=body"`
	Status string
}

// sqHookTypes keeps hook types linked into the test binary (a live reflect
// conversion) so xunsafe can resolve them by name, and lets the harness assert
// that resolution picked the intended type.
var sqHookTypes = map[string]reflect.Type{"sqReplaceHooks": reflect.TypeOf(sqReplaceHooks{})}

// sqReplaceHooks implements atomic child replacement: children present in
// Previous but absent from the request are appended as explicit deletions.
type sqReplaceHooks struct{}

func (*sqReplaceHooks) Init(_ context.Context, entity *sqParent, state xhandler.LifecycleContext[sqParent, xhandler.NoParent, sqOutput]) error {
	if state.Previous == nil {
		return nil
	}
	for _, previous := range state.Previous.Children {
		state.Output.Events = append(state.Output.Events, fmt.Sprintf("previous:%d", *previous.ID))
	}
	for _, previous := range state.Previous.Children {
		kept := false
		for _, child := range entity.Children {
			if child.ID != nil && *child.ID == *previous.ID {
				kept = true
			}
		}
		if kept {
			continue
		}
		id, version := *previous.ID, *previous.Version
		entity.Children = append(entity.Children, &sqChild{ID: &id, Version: &version, Remove: true, Has: &sqChildHas{ID: true, Version: true, Remove: true}})
	}
	return nil
}

// --- harness ----------------------------------------------------------------

type sqBinder struct{ data *sqldml.Data }

func (b *sqBinder) Bind(context.Context, any) error { return nil }
func (b *sqBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	switch key {
	case xhandler.FrameworkValidatorKey:
		return b.data.FrameworkValidator(), true, nil
	case xhandler.DMLKey, xhandler.SequencerKey, xhandler.TransactionStarterKey:
		return b.data, true, nil
	}
	return nil, false, nil
}

func sqComponent(scope string, operation string, hooks string) *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: "Rows"}, Name: "Rows",
		Settings: &spec.Settings{Mutation: operation},
		RootView: &spec.View{Name: "Rows", EntityHooks: hooks, Source: &spec.ViewSource{Table: "parents"},
			Columns:   []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}},
			Relations: []*spec.Relation{{Holder: "Children", Name: "Children", View: &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}}},
	}
}

// runSQLiteWriter executes the universal writer against sqlite with real DML,
// sequencing and framework validation, then flushes the invocation journal.
func runSQLiteWriter(t *testing.T, ctx context.Context, db *sqlite.Harness, component *spec.Component, input any, output any, operation string) (any, error) {
	t.Helper()
	inputType, outputType := reflect.TypeOf(input).Elem(), reflect.TypeOf(output).Elem()
	handler, err := New(component, inputType, outputType, operation)
	if err != nil {
		t.Fatalf("compile writer: %v", err)
	}
	data := sqldml.NewData(db.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := handler.CaptureInput(ctx, input)
	if err != nil {
		_ = data.Complete(ctx, err)
		return nil, err
	}
	invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}}
	result, err := handler.Execute(ctx, invocation)
	if completeErr := data.Complete(ctx, err); err == nil && completeErr != nil {
		err = completeErr
	}
	return result, err
}

type sqRow struct {
	ID       int
	ParentID *int
	Label    *string
	Version  *int
}

func sqChildren(t *testing.T, ctx context.Context, db *sqlite.Harness) []sqRow {
	t.Helper()
	rows, err := db.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id, parent_id, label, version FROM children ORDER BY id"}, reflect.TypeOf([]sqRow{}))
	if err != nil {
		t.Fatal(err)
	}
	return rows.([]sqRow)
}

func sqParents(t *testing.T, ctx context.Context, db *sqlite.Harness) []sqRow {
	t.Helper()
	rows, err := db.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id, name AS label, version FROM parents ORDER BY id"}, reflect.TypeOf([]sqRow{}))
	if err != nil {
		t.Fatal(err)
	}
	return rows.([]sqRow)
}

func sqLoadCurrent(t *testing.T, ctx context.Context, db *sqlite.Harness, parents any, children any) {
	t.Helper()
	load := func(sql string, dest any) {
		rows, err := db.ReadQuery(ctx, sqlite.Query{SQL: sql}, reflect.TypeOf(dest).Elem())
		if err != nil {
			t.Fatal(err)
		}
		reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(rows))
	}
	versioned := func(dest any) string {
		if _, ok := reflect.TypeOf(dest).Elem().Elem().Elem().FieldByName("Version"); ok {
			return ", version"
		}
		return ""
	}
	load("SELECT id, name"+versioned(parents)+" FROM parents ORDER BY id", parents)
	load("SELECT id, parent_id AS ParentID, label"+versioned(children)+" FROM children ORDER BY id", children)
}

// sqObserveWrites counts row changes per business table. sqlite keeps a single
// update hook per connection, so one callback serves every table.
func sqObserveWrites(t *testing.T, ctx context.Context, db *sqlite.Harness) map[string]*atomic.Int64 {
	t.Helper()
	counts := map[string]*atomic.Int64{"parents": {}, "children": {}}
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		raw.(*sqlite3.SQLiteConn).RegisterUpdateHook(func(_ int, _ string, table string, _ int64) {
			if count, ok := counts[table]; ok {
				count.Add(1)
			}
		})
		return nil
	})
	if closeErr := conn.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return counts
}
func ptr[T any](value T) *T { return &value }

const sqSchema = `PRAGMA foreign_keys = ON;
CREATE TABLE parents(id INTEGER PRIMARY KEY, name TEXT, version INTEGER);
CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id), label TEXT, version INTEGER);`

// --- data driven cases ------------------------------------------------------

func TestUniversalWriterSQLite(t *testing.T) {
	scope := reflect.TypeFor[sqParent]().PkgPath()
	type expectation struct {
		parents, children []sqRow
		parentWrites      int64
		childWrites       int64
		errContains       string
		events            []string
	}
	cases := []struct {
		name      string
		operation string
		hooks     string
		seed      []string
		plain     bool
		input     func(ctx context.Context, t *testing.T, db *sqlite.Harness) any
		output    func() any
		repeat    int
		want      expectation
	}{
		{
			// Issue 2: link reconciliation must not turn an identity-only child
			// into an UPDATE of its (unchanged) foreign key.
			name: "patch identity-only child is a no-op", operation: "patch", plain: true,
			seed: []string{"INSERT INTO parents VALUES(1,'p',NULL)", "INSERT INTO children VALUES(10,1,'c',NULL)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true, Children: true},
					Children: []*sqPlainChild{{ID: ptr(10), Has: &sqPlainChildHas{ID: true}}}}}}
				var parents []*sqPlainParent
				var children []*sqPlainChild
				sqLoadCurrent(t, ctx, db, &parents, &children)
				in.CurrentRows, in.CurrentChildren = parents, children
				return in
			},
			output: func() any { return &sqPlainOutput{} },
			want:   expectation{parents: []sqRow{{ID: 1, Label: ptr("p")}}, children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("c")}}, parentWrites: 0, childWrites: 0},
		},
		{
			// Nesting a child under a different parent than Previous is rejected by
			// the scope guard rather than silently re-parented.
			name: "patch child nested under a different parent is rejected", operation: "patch", plain: true,
			seed: []string{"INSERT INTO parents VALUES(1,'p',NULL)", "INSERT INTO parents VALUES(2,'q',NULL)", "INSERT INTO children VALUES(10,1,'c',NULL)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(2), Has: &sqPlainParentHas{ID: true, Children: true},
					Children: []*sqPlainChild{{ID: ptr(10), Has: &sqPlainChildHas{ID: true}}}}}}
				var parents []*sqPlainParent
				var children []*sqPlainChild
				sqLoadCurrent(t, ctx, db, &parents, &children)
				in.CurrentRows, in.CurrentChildren = parents, children
				return in
			},
			output: func() any { return &sqPlainOutput{} },
			want:   expectation{errContains: "matched Previous outside parent scope", children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("c")}}},
		},
		{
			name: "patch child value without concurrency token conflicts", operation: "patch",
			seed: []string{"INSERT INTO parents VALUES(1,'p',1)", "INSERT INTO children VALUES(10,1,'c',1)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqInput{Rows: []*sqParent{{ID: ptr(1), Has: &sqParentHas{ID: true, Children: true},
					Children: []*sqChild{{ID: ptr(10), Label: ptr("changed"), Has: &sqChildHas{ID: true, Label: true}}}}}}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				return in
			},
			output: func() any { return &sqOutput{} },
			want:   expectation{errContains: "expected token is missing", children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("c"), Version: ptr(1)}}},
		},
		{
			// The parent token guards the aggregate: it is supplied and matches, so
			// the parent itself is not written while the child update is atomic.
			name: "patch child value with matching tokens updates atomically", operation: "patch",
			seed: []string{"INSERT INTO parents VALUES(1,'p',1)", "INSERT INTO children VALUES(10,1,'c',1)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqInput{Rows: []*sqParent{{ID: ptr(1), Version: ptr(1), Has: &sqParentHas{ID: true, Version: true, Children: true},
					Children: []*sqChild{{ID: ptr(10), Label: ptr("changed"), Version: ptr(1), Has: &sqChildHas{ID: true, Label: true, Version: true}}}}}}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				return in
			},
			output: func() any { return &sqOutput{} },
			want:   expectation{children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("changed"), Version: ptr(1)}}, childWrites: 1, parentWrites: 0},
		},
		{
			name: "patch child under parent without its token conflicts (aggregate guard)", operation: "patch",
			seed: []string{"INSERT INTO parents VALUES(1,'p',1)", "INSERT INTO children VALUES(10,1,'c',1)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqInput{Rows: []*sqParent{{ID: ptr(1), Has: &sqParentHas{ID: true, Children: true},
					Children: []*sqChild{{ID: ptr(10), Label: ptr("changed"), Version: ptr(1), Has: &sqChildHas{ID: true, Label: true, Version: true}}}}}}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				return in
			},
			output: func() any { return &sqOutput{} },
			want:   expectation{errContains: "Rows.Version: expected token is missing", children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("c"), Version: ptr(1)}}},
		},
		{
			// Issue 1: rows appended by Init (atomic replacement) have no
			// captured original; token-bearing deletions must still work.
			name: "init-appended deletions of token-bearing children succeed", operation: "patch", hooks: "sqReplaceHooks",
			seed: []string{"INSERT INTO parents VALUES(1,'p',1)", "INSERT INTO children VALUES(10,1,'keep',1)", "INSERT INTO children VALUES(11,1,'drop',3)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqInput{Rows: []*sqParent{{ID: ptr(1), Version: ptr(1), Has: &sqParentHas{ID: true, Version: true, Children: true},
					Children: []*sqChild{{ID: ptr(10), Label: ptr("kept"), Version: ptr(1), Has: &sqChildHas{ID: true, Label: true, Version: true}}}}}}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				return in
			},
			output: func() any { return &sqOutput{} },
			want:   expectation{children: []sqRow{{ID: 10, ParentID: ptr(1), Label: ptr("kept"), Version: ptr(1)}}, childWrites: 2, events: []string{"previous:10", "previous:11"}},
		},
		{
			// Issue 4: Previous relation assembly must preserve the loaded
			// Current order run after run.
			name: "previous children keep current order", operation: "patch", hooks: "sqReplaceHooks", repeat: 25,
			seed: []string{"INSERT INTO parents VALUES(1,'p',1)",
				"INSERT INTO children VALUES(13,1,'a',1)", "INSERT INTO children VALUES(10,1,'b',1)", "INSERT INTO children VALUES(15,1,'c',1)",
				"INSERT INTO children VALUES(11,1,'d',1)", "INSERT INTO children VALUES(14,1,'e',1)", "INSERT INTO children VALUES(12,1,'f',1)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqInput{Rows: []*sqParent{{ID: ptr(1), Version: ptr(1), Has: &sqParentHas{ID: true, Version: true, Children: true},
					Children: []*sqChild{}}}}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				// Current children arrive in a deliberately non-id order.
				sort.SliceStable(in.CurrentChildren, func(i, j int) bool { return *in.CurrentChildren[i].Label < *in.CurrentChildren[j].Label })
				return in
			},
			output: func() any { return &sqOutput{} },
			want:   expectation{children: []sqRow{}, childWrites: 6, events: []string{"previous:13", "previous:10", "previous:15", "previous:11", "previous:14", "previous:12"}},
		},
		{
			name: "post allocates identities for parent and child", operation: "post", plain: true,
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				return &sqPlainInput{Rows: []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true, Children: true},
					Children: []*sqPlainChild{{Label: ptr("first"), Has: &sqPlainChildHas{Label: true}}, {Label: ptr("second"), Has: &sqPlainChildHas{Label: true}}}}}}
			},
			output: func() any { return &sqPlainOutput{} },
			want:   expectation{parents: []sqRow{{ID: 1, Label: ptr("new")}}, children: []sqRow{{ID: 1, ParentID: ptr(1), Label: ptr("first")}, {ID: 2, ParentID: ptr(1), Label: ptr("second")}}, parentWrites: 1, childWrites: 2},
		},
		{
			name: "patch deletes children before their parent", operation: "patch", plain: true,
			seed: []string{"INSERT INTO parents VALUES(1,'p',NULL)", "INSERT INTO children VALUES(10,1,'c',NULL)", "INSERT INTO children VALUES(11,1,'d',NULL)"},
			input: func(ctx context.Context, t *testing.T, db *sqlite.Harness) any {
				in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Remove: true, Has: &sqPlainParentHas{ID: true, Remove: true, Children: true},
					Children: []*sqPlainChild{{ID: ptr(10), Remove: true, Has: &sqPlainChildHas{ID: true, Remove: true}}, {ID: ptr(11), Remove: true, Has: &sqPlainChildHas{ID: true, Remove: true}}}}}}
				var parents []*sqPlainParent
				var children []*sqPlainChild
				sqLoadCurrent(t, ctx, db, &parents, &children)
				in.CurrentRows, in.CurrentChildren = parents, children
				return in
			},
			output: func() any { return &sqPlainOutput{} },
			want:   expectation{parents: []sqRow{}, children: []sqRow{}, parentWrites: 1, childWrites: 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repeat := tc.repeat
			if repeat == 0 {
				repeat = 1
			}
			for iteration := 0; iteration < repeat; iteration++ {
				ctx := context.Background()
				db := sqlite.New(t)
				// One connection so the sqlite update hook observes every write.
				db.DB.SetMaxOpenConns(1)
				statements := append(strings.Split(sqSchema, "\n"), tc.seed...)
				if err := db.ExecStatements(ctx, statements...); err != nil {
					t.Fatal(err)
				}
				writes := sqObserveWrites(t, ctx, db)
				component := sqComponent(scope, tc.operation, tc.hooks)
				input, output := tc.input(ctx, t, db), tc.output()
				if tc.hooks != "" {
					if metadata, err := Compile(component, reflect.TypeOf(input).Elem(), reflect.TypeOf(output).Elem(), tc.operation); err != nil || metadata.HookType != sqHookTypes[tc.hooks] {
						t.Fatalf("hook type %s was not resolved (err=%v)", tc.hooks, err)
					}
				}
				result, err := runSQLiteWriter(t, ctx, db, component, input, output, tc.operation)
				if tc.want.errContains != "" {
					if err == nil || !strings.Contains(err.Error(), tc.want.errContains) {
						t.Fatalf("expected error containing %q, got %v", tc.want.errContains, err)
					}
				} else if err != nil {
					t.Fatalf("writer failed: %v", err)
				}
				if tc.want.parents != nil {
					if got := sqParents(t, ctx, db); !reflect.DeepEqual(got, tc.want.parents) {
						t.Fatalf("parents = %s, want %s", sqDump(got), sqDump(tc.want.parents))
					}
				}
				if tc.want.children != nil {
					if got := sqChildren(t, ctx, db); !reflect.DeepEqual(got, tc.want.children) {
						t.Fatalf("children = %s, want %s", sqDump(got), sqDump(tc.want.children))
					}
				}
				if tc.want.errContains == "" {
					if got := writes["parents"].Load(); got != tc.want.parentWrites {
						t.Fatalf("parent writes = %d, want %d", got, tc.want.parentWrites)
					}
					if got := writes["children"].Load(); got != tc.want.childWrites {
						t.Fatalf("child writes = %d, want %d", got, tc.want.childWrites)
					}
				}
				if tc.want.events != nil {
					if result == nil {
						t.Fatalf("iteration %d produced no output", iteration)
					}
					events := reflect.ValueOf(result).Elem().FieldByName("Events").Interface().([]string)
					if !reflect.DeepEqual(events, tc.want.events) {
						t.Fatalf("iteration %d events = %v, want %v", iteration, events, tc.want.events)
					}
				}
			}
		})
	}
}

func sqDump(rows []sqRow) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		part := fmt.Sprintf("{id:%d", row.ID)
		if row.ParentID != nil {
			part += fmt.Sprintf(" parent:%d", *row.ParentID)
		}
		if row.Label != nil {
			part += fmt.Sprintf(" label:%s", *row.Label)
		}
		if row.Version != nil {
			part += fmt.Sprintf(" version:%d", *row.Version)
		}
		parts = append(parts, part+"}")
	}
	return "[" + strings.Join(parts, " ") + "]"
}
