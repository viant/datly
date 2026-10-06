package engine_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type physicalRoleSharedRow struct {
	ID       int    `sqlx:"ID,primaryKey=true"`
	Name     string `sqlx:"NAME"`
	ParentID int    `sqlx:"PARENT_ID"`
}
type physicalRoleRoot struct {
	ID    int                      `sqlx:"ID,primaryKey=true"`
	Left  []*physicalRoleSharedRow `view:"Left,table=left_rows" on:"ID=ParentID"`
	Right []*physicalRoleSharedRow `view:"Right,table=right_rows" on:"ID=ParentID"`
}
type physicalRoleInput struct {
	Rows         []*physicalRoleRoot      `parameter:"Rows,kind=body,in=data" view:"Rows,table=carrier,auxiliary=true"`
	CurrentRows  []*physicalRoleRoot      `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=carrier"`
	CurrentLeft  []*physicalRoleSharedRow `parameter:"CurrentLeft,kind=view" view:"CurrentLeft,table=left_rows"`
	CurrentRight []*physicalRoleSharedRow `parameter:"CurrentRight,kind=view" view:"CurrentRight,table=right_rows"`
}
type physicalRoleOutput struct {
	Data []*physicalRoleRoot `parameter:"Data,kind=output,in=body"`
}
type physicalRoleDML struct {
	*dml.Data
	tables []string
}

func (d *physicalRoleDML) Insert(table string, v any) error {
	d.tables = append(d.tables, table)
	return d.Data.Insert(table, v)
}

type physicalRoleBinder struct{ data *physicalRoleDML }

func (*physicalRoleBinder) Bind(context.Context, any) error { return nil }
func (b *physicalRoleBinder) Lookup(_ context.Context, k xhandler.ValueKey) (any, bool, error) {
	switch k {
	case xhandler.FrameworkValidatorKey:
		return b.data.FrameworkValidator(), true, nil
	case xhandler.DMLKey, xhandler.SequencerKey, xhandler.TransactionStarterKey:
		return b.data, true, nil
	}
	return nil, false, nil
}

// A native SQLite fixture with two graph roles sharing their public pointer.
// Caller-supplied identity41 is retained; no ID synthesis or fake Current.
func TestNativeSharedPointerTwoPhysicalRolesOwnership(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, sql := range []string{"CREATE TABLE carrier(ID INTEGER PRIMARY KEY)", "CREATE TABLE left_rows(ID INTEGER PRIMARY KEY,NAME TEXT,PARENT_ID INTEGER REFERENCES carrier(ID))", "CREATE TABLE right_rows(ID INTEGER PRIMARY KEY,NAME TEXT,PARENT_ID INTEGER REFERENCES carrier(ID))", "INSERT INTO carrier(ID)VALUES(1)"} {
		if _, err = db.ExecContext(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	row := &physicalRoleSharedRow{ID: 41, Name: "shared", ParentID: 1}
	root := &physicalRoleRoot{ID: 1, Left: []*physicalRoleSharedRow{row}, Right: []*physicalRoleSharedRow{row}}
	var currentID int
	if err = db.QueryRowContext(ctx, "SELECT ID FROM carrier").Scan(&currentID); err != nil {
		t.Fatal(err)
	}
	readCurrent := func(table string) []*physicalRoleSharedRow {
		rows, e := db.QueryContext(ctx, "SELECT ID,NAME FROM "+table)
		if e != nil {
			t.Fatal(e)
		}
		defer rows.Close()
		out := []*physicalRoleSharedRow{}
		for rows.Next() {
			r := new(physicalRoleSharedRow)
			if e = rows.Scan(&r.ID, &r.Name); e != nil {
				t.Fatal(e)
			}
			out = append(out, r)
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		return out
	}
	input := &physicalRoleInput{Rows: []*physicalRoleRoot{root}, CurrentRows: []*physicalRoleRoot{{ID: currentID}}, CurrentLeft: readCurrent("left_rows"), CurrentRight: readCurrent("right_rows")}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Rows"}, Name: "Rows", Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "carrier"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}}}}
	h, err := writer.New(c, reflect.TypeFor[physicalRoleInput](), reflect.TypeFor[physicalRoleOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	data := &physicalRoleDML{Data: dml.NewData(db)}
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.CaptureInput(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &physicalRoleBinder{data: data}})
	complete := data.Complete(ctx, err)
	if err == nil {
		err = complete
	}
	var left, right int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*)FROM left_rows").Scan(&left)
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*)FROM right_rows").Scan(&right)
	if !reflect.DeepEqual(data.tables, []string{"left_rows", "right_rows"}) || err != nil || left != 1 || right != 1 {
		t.Fatalf("native shared-pointer physical role ownership lost: queued=%v left=%d right=%d err=%v", data.tables, left, right, err)
	}
}

type rolePublicFirst struct {
	ID       int                      `sqlx:"ID,primaryKey=true"`
	Public   []*physicalRoleSharedRow `view:"Public,table=left_rows,auxiliary=true" on:"ID=ParentID"`
	Writable []*physicalRoleSharedRow `view:"Writable,table=left_rows" on:"ID=ParentID"`
}
type rolePublicLast struct {
	ID       int                      `sqlx:"ID,primaryKey=true"`
	Writable []*physicalRoleSharedRow `view:"Writable,table=left_rows" on:"ID=ParentID"`
	Public   []*physicalRoleSharedRow `view:"Public,table=left_rows,auxiliary=true" on:"ID=ParentID"`
}
type roleFirstInput struct {
	Rows        []*rolePublicFirst       `parameter:"Rows,kind=body,in=data" view:"Rows,table=carrier,auxiliary=true"`
	CurrentRows []*rolePublicFirst       `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=carrier"`
	CurrentLeft []*physicalRoleSharedRow `parameter:"CurrentLeft,kind=view" view:"CurrentLeft,table=left_rows"`
}
type roleLastInput struct {
	Rows        []*rolePublicLast        `parameter:"Rows,kind=body,in=data" view:"Rows,table=carrier,auxiliary=true"`
	CurrentRows []*rolePublicLast        `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=carrier"`
	CurrentLeft []*physicalRoleSharedRow `parameter:"CurrentLeft,kind=view" view:"CurrentLeft,table=left_rows"`
}
type roleFirstOutput struct {
	Data   []*rolePublicFirst `parameter:"Data,kind=output,in=body"`
	Queued int
}
type roleLastOutput struct {
	Data   []*rolePublicLast `parameter:"Data,kind=output,in=body"`
	Queued int
}
type roleFirstHook struct{}
type roleLastHook struct{}

var roleOwnershipHookTypes = []reflect.Type{reflect.TypeFor[roleFirstHook](), reflect.TypeFor[roleLastHook]()}

func (*roleFirstHook) AfterQueue(_ context.Context, _ *physicalRoleSharedRow, s xhandler.LifecycleContext[physicalRoleSharedRow, rolePublicFirst, roleFirstOutput]) error {
	s.Output.Queued++
	return nil
}
func (*roleLastHook) AfterQueue(_ context.Context, _ *physicalRoleSharedRow, s xhandler.LifecycleContext[physicalRoleSharedRow, rolePublicLast, roleLastOutput]) error {
	s.Output.Queued++
	return nil
}

func TestNativeSharedPointerAuxiliaryPhysicalTraversalOrders(t *testing.T) {
	for _, order := range []string{"public-first", "public-last"} {
		t.Run(order, func(t *testing.T) {
			_ = roleOwnershipHookTypes
			ctx := context.Background()
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			for _, q := range []string{"PRAGMA foreign_keys=ON", "CREATE TABLE carrier(ID INTEGER PRIMARY KEY)", "CREATE TABLE left_rows(ID INTEGER PRIMARY KEY,NAME TEXT,PARENT_ID INTEGER REFERENCES carrier(ID))", "INSERT INTO carrier VALUES(1)"} {
				if _, err = db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			var currentID int
			if err = db.QueryRow("SELECT ID FROM carrier").Scan(&currentID); err != nil {
				t.Fatal(err)
			}
			current := []*physicalRoleSharedRow{}
			rs, err := db.Query("SELECT ID,NAME,PARENT_ID FROM left_rows")
			if err != nil {
				t.Fatal(err)
			}
			for rs.Next() {
				r := new(physicalRoleSharedRow)
				if err = rs.Scan(&r.ID, &r.Name, &r.ParentID); err != nil {
					t.Fatal(err)
				}
				current = append(current, r)
			}
			if err = rs.Err(); err != nil {
				t.Fatal(err)
			}
			rs.Close()
			row := &physicalRoleSharedRow{ID: 41, Name: "shared", ParentID: 1}
			var in any
			var out reflect.Type
			hook := "roleFirstHook"
			var public, physical []*physicalRoleSharedRow
			if order == "public-first" {
				root := &rolePublicFirst{ID: 1, Public: []*physicalRoleSharedRow{row}, Writable: []*physicalRoleSharedRow{row}}
				in = &roleFirstInput{Rows: []*rolePublicFirst{root}, CurrentRows: []*rolePublicFirst{{ID: currentID}}, CurrentLeft: current}
				out = reflect.TypeFor[roleFirstOutput]()
				public, physical = root.Public, root.Writable
			} else {
				root := &rolePublicLast{ID: 1, Public: []*physicalRoleSharedRow{row}, Writable: []*physicalRoleSharedRow{row}}
				in = &roleLastInput{Rows: []*rolePublicLast{root}, CurrentRows: []*rolePublicLast{{ID: currentID}}, CurrentLeft: current}
				out = reflect.TypeFor[roleLastOutput]()
				hook = "roleLastHook"
				public, physical = root.Public, root.Writable
			}
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeOf(in).Elem().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "carrier"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}}, Relations: []*spec.Relation{{Name: "Writable", Holder: "Writable", View: &spec.View{Name: "Writable", EntityHooks: hook, Source: &spec.ViewSource{Table: "left_rows"}}}}}}
			h, err := writer.New(c, reflect.TypeOf(in).Elem(), out, "patch")
			if err != nil {
				t.Fatal(err)
			}
			data := &physicalRoleDML{Data: dml.NewData(db)}
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			snapshot, err := h.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			result, err := h.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &physicalRoleBinder{data: data}})
			complete := data.Complete(ctx, err)
			if err == nil {
				err = complete
			}
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err = db.QueryRow("SELECT COUNT(*) FROM left_rows WHERE ID=41 AND PARENT_ID=1").Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 1 || !reflect.DeepEqual(data.tables, []string{"left_rows"}) || reflect.ValueOf(result).Elem().FieldByName("Queued").Int() != 1 || public[0] != row || physical[0] != row || row.ID != 41 {
				t.Fatalf("role ownership/order lost: tables=%v rows=%d output=%+v", data.tables, n, result)
			}
		})
	}
}
