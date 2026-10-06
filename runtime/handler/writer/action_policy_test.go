package writer

import (
	"context"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type replacementHas struct{ ID, Name, Remove bool }
type replacementRow struct {
	ID     *int            `sqlx:"id,primaryKey=true"`
	Name   *string         `sqlx:"name"`
	Remove bool            `sqlx:"-" writer:"delete"`
	Has    *replacementHas `setMarker:"true" sqlx:"-"`
}

func TestInsertDeleteActionPolicyFailureIsAtomicSQLite(t *testing.T) {
	for _, name := range []string{"unpaired insert collides", "missing delete", "incomplete insert", "late insert trigger", "duplicate deletes before insert", "three-row replacement"} {
		t.Run(name, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE audit(action TEXT NOT NULL)", "INSERT INTO items VALUES(1,'old')", "CREATE TRIGGER item_removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
			remove := &replacementRow{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}}
			insert := &replacementRow{ID: ptr(1), Name: ptr("new"), Has: &replacementHas{ID: true, Name: true}}
			in := &replacementInput{Rows: []*replacementRow{remove, insert}, CurrentRows: []*replacementRow{{ID: ptr(1), Name: ptr("old")}}}
			switch name {
			case "unpaired insert collides":
				in.Rows = []*replacementRow{insert}
			case "missing delete":
				remove.ID = ptr(9)
			case "incomplete insert":
				insert.Name = nil
				insert.Has.Name = false
			case "duplicate deletes before insert", "three-row replacement":
				duplicate := &replacementRow{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}}
				in.Rows = []*replacementRow{remove, duplicate, insert}
				if name == "three-row replacement" {
					in.Rows = []*replacementRow{insert, remove, duplicate}
				}
			case "late insert trigger":
				if err := db.ExecStatements(ctx, "CREATE TRIGGER reject_insert AFTER INSERT ON items BEGIN SELECT RAISE(ABORT,'late insert rejected'); END"); err != nil {
					t.Fatal(err)
				}
			}
			_, err := runSQLiteWriter(t, ctx, db, component, in, &replacementOutput{}, "patch")
			if err == nil {
				t.Fatal("invalid replacement succeeded")
			}
			var stored string
			var events int
			if err = db.DB.QueryRow("SELECT name FROM items WHERE id=1").Scan(&stored); err != nil || stored != "old" {
				t.Fatalf("stored=%q err=%v", stored, err)
			}
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 0 {
				t.Fatalf("events=%d err=%v", events, err)
			}
		})
	}
}

func TestInsertDeleteActionPolicyRetainsPreviousWithoutUpdateEvidence(t *testing.T) {
	key := Field{Name: "ID", Column: "id", Index: []int{0}}
	name := Field{Name: "Name", Column: "name", Index: []int{1}}
	record := &Record{WriterActionPolicy: insertDeleteActionPolicy, EntityType: reflect.TypeFor[replacementRow](), Keys: []Field{key}, Fields: []Field{key, name}, Invariants: map[string][]Field{"pair": {key, name}}}
	current := &replacementRow{ID: ptr(1), Has: &replacementHas{ID: true}}
	previous := reflect.ValueOf(&replacementRow{ID: ptr(1), Name: ptr("old")})
	frame := &Frame{Record: record, Entity: reflect.ValueOf(current), Previous: previous, Action: xhandler.WriteInsert, Fields: livePresence(record, reflect.ValueOf(current).Elem())}
	p := &Program{frames: &MutationFrames{Rows: []*Frame{frame}}}
	options := p.validationOptions(frame, false)
	if options.Previous != nil || options.PreviousFields != nil || options.Fields != nil {
		t.Fatalf("insert carries update evidence: %+v", options)
	}
	if err := p.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if current.Name != nil || !frame.Previous.IsValid() || frame.Previous != previous {
		t.Fatal("insert backfilled data or erased authoritative Previous")
	}
}

type replacementInput struct {
	Rows        []*replacementRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=items"`
	CurrentRows []*replacementRow `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=items"`
}
type replacementOutput struct {
	Data []*replacementRow `parameter:"Data,kind=output,in=body"`
}

func TestInsertDeleteActionPolicySameIdentitySQLite(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		rows, events int
	}{
		{"default matched row remains update", "", 0, 1},
		{"explicit replacement inserts after delete", "insert-delete", 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx,
				"CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)",
				"CREATE TABLE audit(action TEXT NOT NULL)",
				"INSERT INTO items VALUES(1,'old')",
				"CREATE TRIGGER item_removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END",
				"CREATE TRIGGER item_added AFTER INSERT ON items BEGIN INSERT INTO audit VALUES('inserted'); END"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: tc.policy, Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
			in := &replacementInput{Rows: []*replacementRow{
				{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}},
				{ID: ptr(1), Name: ptr("new"), Has: &replacementHas{ID: true, Name: true}},
			}, CurrentRows: []*replacementRow{{ID: ptr(1), Name: ptr("old")}}}
			_, err := runSQLiteWriter(t, ctx, db, component, in, &replacementOutput{}, "patch")
			if err != nil {
				t.Fatal(err)
			}
			var rows, events int
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil {
				t.Fatal(err)
			}
			if rows != tc.rows || events != tc.events {
				t.Fatalf("rows=%d events=%d, want %d/%d", rows, events, tc.rows, tc.events)
			}
			if tc.rows == 1 {
				var name string
				if err = db.DB.QueryRow("SELECT name FROM items WHERE id=1").Scan(&name); err != nil || name != "new" {
					t.Fatalf("name=%q err=%v", name, err)
				}
			}
		})
	}
}

func TestInsertDeletePolicyRejectsGraphScopedRecovery(t *testing.T) {
	for _, scope := range []string{"ancestor", "sibling"} {
		t.Run(scope, func(t *testing.T) {
			leaf := &Record{Name: "Leaf", WriterActionPolicy: insertDeleteActionPolicy, Table: "items", CurrentField: 0, Keys: []Field{{Name: "ID"}}, DeleteMarker: &Field{Name: "Remove"}}
			root := &Record{Name: "Root", Auxiliary: true, Relations: []*Relation{{Child: leaf}}}
			if scope == "ancestor" {
				root.ScopedSequences = []ScopedSequence{{}}
			} else {
				root.Relations = append(root.Relations, &Relation{Child: &Record{Name: "Sibling", Table: "other", ScopedSequences: []ScopedSequence{{}}}})
			}
			if err := validateWriterActionPolicy(root, "patch"); err == nil {
				t.Fatal("graph-wide scoped recovery combined with insert-delete policy was admitted")
			}
		})
	}
}

func TestInsertDeleteActionPolicyCrossRoleCollision(t *testing.T) {
	key := Field{Name: "ID", Index: []int{0}}
	left := &Record{Path: "Left", Table: "items", WriterActionPolicy: insertDeleteActionPolicy, Keys: []Field{key}}
	right := &Record{Path: "Right", Table: "items", WriterActionPolicy: insertDeleteActionPolicy, Keys: []Field{key}}
	remove := reflect.ValueOf(&replacementRow{ID: ptr(1), Remove: true})
	insert := reflect.ValueOf(&replacementRow{ID: ptr(1), Name: ptr("new")})
	p := &Program{metadata: &Metadata{Root: &Record{Auxiliary: true, Relations: []*Relation{{Child: left}, {Child: right}}}}, frames: &MutationFrames{Rows: []*Frame{{Record: left, Entity: remove}, {Record: right, Entity: insert}}}, actions: &MutationActions{Rows: []*Action{{Kind: xhandler.WriteDelete, Entity: remove}, {Kind: xhandler.WriteInsert, Entity: insert}}}}
	for i, action := range p.actions.Rows {
		action.frame = p.frames.Rows[i]
	}
	if err := p.validateActionPolicyActions(); err == nil {
		t.Fatal("different roles shared a physical replacement")
	}
	p.frames.Rows[1].Record = left
	if err := p.validateActionPolicyActions(); err != nil {
		t.Fatalf("genuine same-role pair rejected: %v", err)
	}
}

// These hooks mutate the native writer's working row, not a replay source.
type replacementMutationKey struct{}
type replacementMutation struct {
	phase, field string
	input        *replacementInput
}
type replacementMutationHooks struct{}

func (*replacementMutationHooks) mutate(ctx context.Context, phase string, row *replacementRow) {
	cfg, _ := ctx.Value(replacementMutationKey{}).(replacementMutation)
	if cfg.phase != phase {
		return
	}
	if cfg.field == "append" {
		if row.Remove {
			cfg.input.Rows = append(cfg.input.Rows, &replacementRow{ID: ptr(3), Name: ptr("added"), Has: &replacementHas{ID: true, Name: true}})
		}
		return
	}
	if cfg.field == "ID" {
		*row.ID = 2
	} else {
		row.Remove = !row.Remove
		row.Has.Remove = true
	}
}
func (h *replacementMutationHooks) Init(ctx context.Context, row *replacementRow, _ xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	h.mutate(ctx, "Init", row)
	return nil
}
func (h *replacementMutationHooks) Validate(ctx context.Context, row *replacementRow, _ xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	h.mutate(ctx, "Validate", row)
	return nil
}
func (h *replacementMutationHooks) AfterSequence(ctx context.Context, row *replacementRow, _ xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	h.mutate(ctx, "AfterSequence", row)
	return nil
}
func (h *replacementMutationHooks) AfterQueue(ctx context.Context, row *replacementRow, _ xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	h.mutate(ctx, "AfterQueue", row)
	return nil
}

func TestInsertDeleteNativeCapturedFactsThroughHooks(t *testing.T) {
	for _, phase := range []string{"Init", "Validate", "AfterSequence", "AfterQueue"} {
		for _, field := range []string{"ID", "Remove"} {
			t.Run(phase+"/"+field, func(t *testing.T) {
				ctx := context.WithValue(context.Background(), replacementMutationKey{}, replacementMutation{phase: phase, field: field})
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE audit(action TEXT)", "INSERT INTO items VALUES(1,'authorized'),(2,'unrelated')", "CREATE TRIGGER removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END"); err != nil {
					t.Fatal(err)
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
				handler, err := New(component, reflect.TypeFor[replacementInput](), reflect.TypeFor[replacementOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				handler.metadata.Root.HookType = reflect.TypeFor[replacementMutationHooks]()
				input := &replacementInput{Rows: []*replacementRow{{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}}}, CurrentRows: []*replacementRow{{ID: ptr(1), Name: ptr("authorized")}}}
				data := sqldml.NewData(db.DB)
				if err = data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				snapshot, err := handler.CaptureInput(ctx, input)
				if err == nil {
					_, err = handler.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}})
				}
				if completeErr := data.Complete(ctx, err); err == nil {
					err = completeErr
				}
				if err == nil || !strings.Contains(err.Error(), "captured") {
					t.Fatalf("native captured %s mutation not rejected: %v", field, err)
				}
				var rows, events int
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM items WHERE (id=1 AND name='authorized') OR (id=2 AND name='unrelated')").Scan(&rows); err != nil || rows != 2 {
					t.Fatalf("rows=%d err=%v", rows, err)
				}
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 0 {
					t.Fatalf("audit=%d err=%v", events, err)
				}
			})
		}
	}
}

func TestInsertDeleteNativeInitAddsCapturedRows(t *testing.T) {
	db := sqlite.New(t)
	base := context.Background()
	if err := db.ExecStatements(base, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO items VALUES(1,'old')"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
	handler, err := New(component, reflect.TypeFor[replacementInput](), reflect.TypeFor[replacementOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[replacementMutationHooks]()
	input := &replacementInput{Rows: []*replacementRow{{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}}}, CurrentRows: []*replacementRow{{ID: ptr(1), Name: ptr("old")}}}
	ctx := context.WithValue(base, replacementMutationKey{}, replacementMutation{phase: "Init", field: "append", input: input})
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := handler.CaptureInput(ctx, input)
	if err == nil {
		_, err = handler.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}})
	}
	if completeErr := data.Complete(ctx, err); err == nil {
		err = completeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	var name string
	var count int
	if err = db.DB.QueryRow("SELECT name FROM items WHERE id=3").Scan(&name); err != nil || name != "added" {
		t.Fatalf("insert=%q err=%v", name, err)
	}
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	program := snapshot.(*Program)
	if len(input.Rows) != 2 || len(program.actionPolicyFacts) != 2 {
		t.Fatal("Init addition was not captured exactly once")
	}
}

type replacementRoles struct {
	ID    *int              `sqlx:"id,primaryKey=true"`
	Left  []*replacementRow `view:"Left,table=items" on:"ID=ID"`
	Right []*replacementRow `view:"Right,table=items" on:"ID=ID"`
}
type replacementRolesInput struct {
	Rows         []*replacementRoles `parameter:"Rows,kind=body,in=data" view:"Rows,table=owners"`
	CurrentRows  []*replacementRoles `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=owners"`
	CurrentLeft  []*replacementRow   `parameter:"CurrentLeft,kind=view" view:"CurrentLeft,table=items"`
	CurrentRight []*replacementRow   `parameter:"CurrentRight,kind=view" view:"CurrentRight,table=items"`
}
type replacementRolesOutput struct {
	Data []*replacementRoles `parameter:"Data,kind=output,in=body"`
}

func TestInsertDeleteNativeCrossRolePhysicalCollisionSQLite(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE owners(id INTEGER PRIMARY KEY)", "INSERT INTO owners VALUES(1)", "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO items VALUES(1,'original')", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END"); err != nil {
		t.Fatal(err)
	}
	leaf := func(name string) *spec.View {
		return &spec.View{Name: name, WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRoles]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "owners"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}, Relations: []*spec.Relation{{Holder: "Left", Name: "Left", View: leaf("Left")}, {Holder: "Right", Name: "Right", View: leaf("Right")}}}}
	input := &replacementRolesInput{Rows: []*replacementRoles{{ID: ptr(1), Left: []*replacementRow{{ID: ptr(1), Remove: true, Has: &replacementHas{ID: true, Remove: true}}}, Right: []*replacementRow{{ID: ptr(1), Name: ptr("replacement"), Has: &replacementHas{ID: true, Name: true}}}}}, CurrentRows: []*replacementRoles{{ID: ptr(1)}}, CurrentLeft: []*replacementRow{{ID: ptr(1), Name: ptr("original")}}, CurrentRight: []*replacementRow{{ID: ptr(1), Name: ptr("original")}}}
	_, err := runSQLiteWriter(t, ctx, db, component, input, &replacementRolesOutput{}, "patch")
	if err == nil || !strings.Contains(err.Error(), "collides between writer roles") {
		t.Fatalf("native cross-role collision=%v", err)
	}
	var name string
	var events int
	if err = db.DB.QueryRow("SELECT name FROM items WHERE id=1").Scan(&name); err != nil || name != "original" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 0 {
		t.Fatalf("events=%d err=%v", events, err)
	}
}

type replacementBoundaryHooks struct {
	input   *replacementInput
	mode    string
	changed bool
}

func (h *replacementBoundaryHooks) Init(context.Context, *replacementRow, xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	if h.mode == "Init replace storage" && !h.changed {
		h.changed = true
		h.input.Rows = []*replacementRow{{ID: ptr(1), Name: ptr("old"), Remove: true, Has: &replacementHas{ID: true, Name: true, Remove: true}}, {ID: ptr(1), Name: ptr("old"), Has: &replacementHas{ID: true, Name: true}}}
	}
	return nil
}
func (h *replacementBoundaryHooks) AfterSequence(context.Context, *replacementRow, xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	if h.mode == "AfterSequence swap slots" && !h.changed {
		h.changed = true
		h.input.Rows[0], h.input.Rows[1] = h.input.Rows[1], h.input.Rows[0]
	}
	return nil
}
func (h *replacementBoundaryHooks) ObservePhase(_ context.Context, event xhandler.PhaseEvent) {
	if h.mode == "Execution end ID" && event.Phase == xhandler.PhaseExecution && event.Boundary == xhandler.PhaseEnd {
		*h.input.Rows[0].ID = 2
		h.changed = true
	}
}
func TestInsertDeleteNativeFrameAndExecutionBoundaryOwnership(t *testing.T) {
	for _, mode := range []string{"Init replace storage", "AfterSequence swap slots", "Execution end ID"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO items VALUES(1,'old'),(2,'unrelated')", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END", "CREATE TRIGGER added AFTER INSERT ON items BEGIN INSERT INTO audit VALUES('inserted'); END"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
			handler, err := New(component, reflect.TypeFor[replacementInput](), reflect.TypeFor[replacementOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.HookType = reflect.TypeFor[replacementBoundaryHooks]()
			input := &replacementInput{Rows: []*replacementRow{{ID: ptr(1), Name: ptr("old"), Remove: true, Has: &replacementHas{ID: true, Name: true, Remove: true}}, {ID: ptr(1), Name: ptr("requested"), Has: &replacementHas{ID: true, Name: true}}}, CurrentRows: []*replacementRow{{ID: ptr(1), Name: ptr("old")}}}
			if mode == "Execution end ID" {
				input.Rows = input.Rows[:1]
			}
			observer := handler.NewPhaseObserver().(*replacementBoundaryHooks)
			observer.input = input
			observer.mode = mode
			ctx, _ = rhandler.WithPhaseObserver(ctx, observer, 0, 0)
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			snapshot, err := handler.CaptureInput(ctx, input)
			if err == nil {
				_, err = handler.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}})
			}
			if completeErr := data.Complete(ctx, err); err == nil {
				err = completeErr
			}
			if err == nil || (!strings.Contains(err.Error(), "captured") && !strings.Contains(err.Error(), "framing")) {
				t.Fatalf("native %s not rejected: %v", mode, err)
			}
			var rows, events int
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM items WHERE (id=1 AND name='old') OR (id=2 AND name='unrelated')").Scan(&rows); err != nil || rows != 2 {
				t.Fatalf("rows=%d err=%v", rows, err)
			}
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 0 {
				t.Fatalf("events=%d err=%v", events, err)
			}
		})
	}
}

type replacementRolesBoundaryHooks struct {
	input   *replacementRolesInput
	changed bool
}

func (h *replacementRolesBoundaryHooks) ObservePhase(_ context.Context, event xhandler.PhaseEvent) {
	if event.Phase == xhandler.PhaseQueue && event.Boundary == xhandler.PhaseBegin {
		*h.input.Rows[1].Right[0].ID = 1
		h.changed = true
	}
}
func TestInsertDeleteNativeMixedRoleQueueObservationCollision(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE owners(id INTEGER PRIMARY KEY)", "INSERT INTO owners VALUES(1),(2)", "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "INSERT INTO items VALUES(1,'old'),(2,'unrelated')", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER removed AFTER DELETE ON items BEGIN INSERT INTO audit VALUES('deleted'); END", "CREATE TRIGGER added AFTER INSERT ON items BEGIN INSERT INTO audit VALUES('inserted'); END"); err != nil {
		t.Fatal(err)
	}
	leaf := func(name, policy string) *spec.View {
		return &spec.View{Name: name, WriterActionPolicy: policy, Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRoles]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "owners"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}, Relations: []*spec.Relation{{Holder: "Left", Name: "Left", View: leaf("Left", "insert-delete")}, {Holder: "Right", Name: "Right", View: leaf("Right", "")}}}}
	input := &replacementRolesInput{Rows: []*replacementRoles{{ID: ptr(1), Left: []*replacementRow{{ID: ptr(1), Name: ptr("new"), Has: &replacementHas{ID: true, Name: true}}}}, {ID: ptr(2), Right: []*replacementRow{{ID: ptr(2), Remove: true, Has: &replacementHas{ID: true, Remove: true}}}}}, CurrentRows: []*replacementRoles{{ID: ptr(1)}, {ID: ptr(2)}}, CurrentLeft: []*replacementRow{{ID: ptr(1), Name: ptr("old")}}, CurrentRight: []*replacementRow{{ID: ptr(2), Name: ptr("unrelated")}}}
	handler, err := New(component, reflect.TypeFor[replacementRolesInput](), reflect.TypeFor[replacementRolesOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[replacementRolesBoundaryHooks]()
	observer := handler.NewPhaseObserver().(*replacementRolesBoundaryHooks)
	observer.input = input
	ctx, _ = rhandler.WithPhaseObserver(ctx, observer, 0, 0)
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := handler.CaptureInput(ctx, input)
	if err == nil {
		_, err = handler.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}})
	}
	if completeErr := data.Complete(ctx, err); err == nil {
		err = completeErr
	}
	if err == nil || !strings.Contains(err.Error(), "captured") {
		t.Fatalf("native mixed-role queue collision not rejected: %v", err)
	}
	var rows, events int
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM items WHERE (id=1 AND name='old') OR (id=2 AND name='unrelated')").Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&events); err != nil || events != 0 {
		t.Fatalf("events=%d err=%v", events, err)
	}
}
