package engine_test

import (
	"context"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/viant/sqlx/testutil/reservationdb"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type eligibleParentHas struct{ ID, Name, Locked, Children bool }
type eligibleParent struct {
	ID       *int               `sqlx:"id,primaryKey,autoincrement"`
	Name     *string            `sqlx:"name" validate:"required"`
	Locked   bool               `sqlx:"-"`
	Children []*eligibleLeaf    `view:"Children,table=eligible_leaf" on:"ID=ParentID"`
	Has      *eligibleParentHas `setMarker:"true" sqlx:"-" json:"-"`
}
type eligibleLeafHas struct{ ID, ParentID, Name, Remove bool }
type eligibleLeaf struct {
	Remove   bool             `sqlx:"-" writer:"delete"`
	ID       *int             `sqlx:"id,primaryKey,autoincrement"`
	ParentID *int             `sqlx:"parent_id,refTable=eligible_parent,refColumn=id"`
	Name     *string          `sqlx:"name" validate:"required"`
	Has      *eligibleLeafHas `setMarker:"true" sqlx:"-" json:"-"`
}
type eligibleParentInput struct {
	CurrentChildren []*eligibleLeaf   `parameter:"CurrentChildren,kind=view,in=CurrentChildren" view:"CurrentChildren,table=eligible_leaf"`
	Rows            []*eligibleParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=eligible_parent"`
	Current         []*eligibleParent `parameter:"Current,kind=view,in=Current" view:"Current,table=eligible_parent"`
}
type eligibleParentOutput struct {
	Data []*eligibleParent `parameter:"Data,kind=output,in=body"`
}
type eligibleParentHook struct{}
type eligibleParentTraceKey struct{}

var eligibleParentLinked = reflect.TypeFor[eligibleParentHook]()

func (*eligibleParentHook) WriteEligible(_ context.Context, row *eligibleParent, _ h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput], _ h.WriteAction) (bool, error) {
	return !row.Locked, nil
}
func (*eligibleParentHook) AfterQueue(ctx context.Context, row *eligibleParent, _ h.LifecycleContext[eligibleParent, h.NoParent, eligibleParentOutput]) error {
	trace := ctx.Value(eligibleParentTraceKey{}).(*[]int)
	*trace = append(*trace, *row.ID)
	return nil
}

func TestNativeEligibilityDirectChildrenSQLite(t *testing.T) { testNativeEligibilityChildren(t, false) }
func TestNativeEligibilityDirectChildrenMySQL(t *testing.T)  { testNativeEligibilityChildren(t, true) }
func testNativeEligibilityChildren(t *testing.T, live bool) {
	for _, tc := range []struct {
		name, body              string
		failure                 bool
		parentNames, childNames []string
		queued                  []int
	}{
		{"suppressed parent retains child", `{"data":[{"ID":1,"Name":"echo only","Locked":true,"Children":[{"Name":"child"}]}]}`, false, []string{"old"}, []string{"child"}, nil},
		{"eligible insert retains foreign key", `{"data":[{"Name":"new","Children":[{"Name":"child"}]}]}`, false, []string{"old", "new"}, []string{"child"}, []int{2}},
		{"excluded insert fails even without children", `{"data":[{"Name":"new","Locked":true}]}`, true, []string{"old"}, []string{}, nil},
		{"invalid root still fails", `{"data":[{"ID":1,"Name":null,"Locked":true,"Children":[{"Name":"child"}]}]}`, true, []string{"old"}, []string{}, nil},
		{"mixed root eligibility", `{"data":[{"ID":1,"Name":"echo only","Locked":true,"Children":[{"Name":"first"}]},{"Name":"new","Children":[{"Name":"second"}]}]}`, false, []string{"old", "new"}, []string{"first", "second"}, []int{2}},
		{"suppressed parent child delete", `{"data":[{"ID":1,"Name":"echo only","Locked":true,"Children":[{"ID":1,"Name":"existing","Remove":true}]}]}`, false, []string{"old"}, []string{}, nil},
		{"child delete rolls back with late insert failure", `{"data":[{"ID":1,"Name":"echo only","Locked":true,"Children":[{"ID":1,"Name":"existing","Remove":true},{"Name":"blocked"}]}]}`, true, []string{"old"}, []string{"existing"}, nil},
		{"late child failure rolls back", `{"data":[{"ID":1,"Name":"echo only","Locked":true,"Children":[{"Name":"first"},{"Name":"blocked"}]}]}`, true, []string{"old"}, []string{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var db *sqlite.Harness
			if live {
				owned := reservationdb.OpenTransient(t)
				config, err := mysql.ParseDSN(owned.DSN)
				if err != nil {
					t.Fatal("invalid MySQL DSN")
				}
				config.DBName = owned.Schema
				conn, err := sql.Open("mysql", config.FormatDSN())
				if err != nil {
					t.Fatal("open isolated MySQL schema")
				}
				t.Cleanup(func() { _ = conn.Close() })
				if expected := os.Getenv("DATLY_TEST_MYSQL_UUID"); expected != "" {
					var actual string
					if err := conn.QueryRow("SELECT @@server_uuid").Scan(&actual); err != nil || actual != expected {
						t.Fatal("MySQL server identity mismatch")
					}
				}
				db = &sqlite.Harness{DB: conn}
			} else {
				db = sqlite.New(t)
			}
			trace := []int(nil)
			ctx := context.WithValue(context.Background(), eligibleParentTraceKey{}, &trace)
			statements := []string{`CREATE TABLE eligible_parent(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)`, `INSERT INTO eligible_parent(id,name) VALUES(1,'old')`, `CREATE TABLE eligible_leaf(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES eligible_parent(id),name TEXT NOT NULL)`, `CREATE TRIGGER fail_leaf BEFORE INSERT ON eligible_leaf WHEN NEW.name='blocked' BEGIN SELECT RAISE(ABORT,'late leaf failure'); END`}
			if live {
				statements = []string{`CREATE TABLE eligible_parent(id BIGINT PRIMARY KEY AUTO_INCREMENT,name VARCHAR(128) NOT NULL) ENGINE=InnoDB`, `INSERT INTO eligible_parent(id,name) VALUES(1,'old')`, `CREATE TABLE eligible_leaf(id BIGINT PRIMARY KEY AUTO_INCREMENT,parent_id BIGINT NOT NULL,name VARCHAR(128) NOT NULL,FOREIGN KEY(parent_id) REFERENCES eligible_parent(id)) ENGINE=InnoDB`, `CREATE TRIGGER fail_leaf BEFORE INSERT ON eligible_leaf FOR EACH ROW BEGIN IF NEW.name='blocked' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='late leaf failure'; END IF; END`}
			}
			if err := db.ExecStatements(ctx, statements...); err != nil {
				t.Fatal(err)
			}

			seedChild := strings.Contains(tc.name, "child delete")
			if seedChild {
				if err := db.ExecStatements(ctx, `INSERT INTO eligible_leaf(id,parent_id,name) VALUES(1,1,'existing')`); err != nil {
					t.Fatal(err)
				}
			}
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: eligibleParentLinked.PkgPath(), Name: "Rows"}, Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/eligible-children"}}, RootView: &spec.View{Name: "Rows", EntityHooks: eligibleParentLinked.Name(), Source: &spec.ViewSource{Table: "eligible_parent"}}}
			compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[eligibleParentInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			in, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/eligible-children"})
			native, err := writer.New(c, reflect.TypeFor[eligibleParentInput](), reflect.TypeFor[eligibleParentOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			id, name := 1, "old"
			provider := handlerprovider.Named("view", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
				if key == "Current" {
					parent := &eligibleParent{ID: &id, Name: &name}
					return []*eligibleParent{parent}, true, nil
				}
				if key == "CurrentChildren" {
					children := []*eligibleLeaf{}
					if seedChild {
						childID, childName := 1, "existing"
						children = append(children, &eligibleLeaf{ID: &childID, ParentID: &id, Name: &childName})
					}
					return children, true, nil
				}
				return nil, false, nil
			})
			request := httptest.NewRequest("PATCH", "/eligible-children", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			output, err := engine.New().Execute(ctx, engine.Request{Input: in, Handler: native, Scope: scope, Providers: []locator.Provider{provider}, DataSource: dml.Source{DB: db.DB}})
			if (err != nil) != tc.failure {
				t.Fatalf("error=%v expected failure=%v", err, tc.failure)
			}
			if strings.Contains(tc.body, `"blocked"`) && (err == nil || !strings.Contains(err.Error(), "late leaf failure")) {
				t.Fatalf("expected physical child trigger failure, got %v", err)
			}
			assertEligibilityNames(t, ctx, db.DB, "eligible_parent", tc.parentNames)
			assertEligibilityNames(t, ctx, db.DB, "eligible_leaf", tc.childNames)
			if !reflect.DeepEqual(trace, tc.queued) {
				t.Fatalf("root queue callbacks %v want %v", trace, tc.queued)
			}
			if !tc.failure {
				rows := output.(*eligibleParentOutput).Data
				wantRows := 1
				if tc.name == "mixed root eligibility" {
					wantRows = 2
				}
				if len(rows) != wantRows {
					t.Fatalf("echo rows=%d want %d", len(rows), wantRows)
				}
				for _, row := range rows {
					for _, child := range row.Children {
						if child.ParentID == nil || *child.ParentID != *row.ID {
							t.Fatal("child parent identity lost")
						}
					}
				}
				if tc.name == "suppressed parent retains child" && *rows[0].Name != "echo only" {
					t.Fatal("suppression changed echoed value")
				}
			}
			if live {
				rows, err := db.DB.QueryContext(ctx, `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME`)
				if err != nil {
					t.Fatal(err)
				}
				tables := []string{}
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						t.Fatal(err)
					}
					tables = append(tables, name)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				if !reflect.DeepEqual(tables, []string{"eligible_leaf", "eligible_parent"}) {
					t.Fatalf("unexpected MySQL tables %v", tables)
				}
			} else {
				assertEligibilityTables(t, ctx, db.DB, "eligible_parent", "eligible_leaf")
			}
		})
	}
}
