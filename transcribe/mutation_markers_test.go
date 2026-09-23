package transcribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedMutationMarkersSQLite(t *testing.T) {
	for _, composite := range []bool{false, true} {
		t.Run(fmt.Sprintf("composite=%v", composite), func(t *testing.T) { runGeneratedMutationMarkers(t, composite) })
	}
}

func runGeneratedMutationMarkers(t *testing.T, composite bool) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	schema := append(append([]string{}, genpatch.Schema...), "ALTER TABLE ORDERS ADD COLUMN VERSION INTEGER NOT NULL DEFAULT 0")
	if composite {
		for i, statement := range schema {
			statement = strings.Replace(statement, "ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID", "TENANT_ID INTEGER NOT NULL DEFAULT 5, ID INTEGER NOT NULL, ORDER_ID", 1)
			if strings.HasPrefix(statement, "CREATE TABLE ITEMS") {
				statement = strings.TrimSuffix(statement, ")") + ", PRIMARY KEY(TENANT_ID,ID))"
			}
			statement = strings.Replace(statement, "INSERT INTO ITEMS VALUES(10,", "INSERT INTO ITEMS VALUES(5,10,", 1)
			schema[i] = statement
		}
	}
	if err := db.ExecStatements(ctx, schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	text := strings.Replace(genpatch.LifecycleDQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, concurrency_token(o.VERSION), delete_marker(o.remove_me), CAST(o.remove_me AS bool), delete_marker(Items.should_delete), CAST(Items.should_delete AS bool),", 1)
	text = strings.Replace(text, "JOIN ITEMS Items", "JOIN (SELECT i.*, '' AS should_delete FROM ITEMS i WHERE i.ID <> 99) Items", 1)
	text = strings.Replace(text, "FROM ORDERS o", "FROM (SELECT r.*, '' AS remove_me FROM ORDERS r) o", 1)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	hooks := filepath.Join(dir, "lifecycle.go")
	original, err := os.ReadFile(hooks)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(original), "return nil", `if remapDelete {for _,item:=range entity.Items {if item.ShouldDelete{id:=11;item.SetId(&id)}}};if advanceVersion && entity.Version != nil { next:=*entity.Version+1;entity.SetVersion(&next) };return nil`, 1) + "\n// application-owned mutation marker test\nvar advanceVersion bool\nvar remapDelete bool\nvar customValidationCalls int\nvar observedDeleteParent bool\n"
	edited = strings.Replace(edited, "func (hooks *OrderRules) Validate", "func (hooks *OrderRules) Validate", 1)
	startValidate := strings.Index(edited, "func (hooks *OrderRules) Validate")
	if startValidate < 0 {
		t.Fatal("root lifecycle signature missing")
	}
	edited = edited[:startValidate] + strings.Replace(edited[startValidate:], "return nil", "customValidationCalls++; return nil", 1)
	startChild := strings.Index(edited, "func (hooks *ItemRules) Validate")
	if startChild < 0 {
		t.Fatal("child lifecycle signature missing")
	}
	edited = edited[:startChild] + strings.Replace(edited[startChild:], "return nil", `customValidationCalls++;if entity.ShouldDelete && state.Parent!=nil && state.Previous!=nil && state.Original.Has("ShouldDelete"){ observedDeleteParent=true };return nil`, 1)
	if err = os.WriteFile(hooks, []byte(edited), 0644); err != nil {
		t.Fatal(err)
	}
	if !composite {
		writeSourceFile(t, root, "source/Orders.dql", text)
		runDatlyTranscribeCLI(t, root, "github.com/viant/datly/genfixture", filepath.Join(db.TempDir, "test.db"), true)
		authored, readErr := os.ReadFile(filepath.Join(root, "source/Orders.dql"))
		if readErr != nil || string(authored) != text {
			t.Fatal("CLI changed authored DQL", readErr)
		}
	}
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatalf("regenerate: %v", err)
	}
	after, _ := os.ReadFile(hooks)
	if string(after) != edited {
		t.Fatalf("regeneration changed application hooks\nBEFORE:\n%s\nAFTER:\n%s", edited, after)
	}
	source := genpatch.RuntimeSource
	// Reuse the shared real binding/runtime/SQLite fixture, replacing its scenario.
	start := strings.Index(source, " actual,err:=invoke(")
	end := strings.Index(source, "\nfunc TestGeneratedSettersDrivePresence")
	source = source[:start] + mutationMarkersRuntime + "\n" + source[end:]
	source = strings.Replace(source, "\"github.com/viant/xdatly/response\"", "\"errors\"\nxhandler \"github.com/viant/xdatly/handler\"", 1)
	source = strings.Replace(source, "\"time\"", "", 1)
	source = strings.Replace(source, "genpatch.Schema...", "append(append([]string{},genpatch.Schema...),\"ALTER TABLE ORDERS ADD COLUMN VERSION INTEGER NOT NULL DEFAULT 0\",\"INSERT INTO ITEMS VALUES(11,1,'preserved')\",\"INSERT INTO ITEMS VALUES(12,1,'remove')\")...", 1)
	source = strings.Replace(source, `"INSERT INTO ITEMS VALUES(12,1,'remove')"`, `"INSERT INTO ITEMS VALUES(12,1,'remove')","INSERT INTO ORDERS VALUES(2,7,'other','2026-09-01T00:00:00Z','2026-09-30T00:00:00Z',0)","INSERT INTO ITEMS VALUES(20,2,'other child')","INSERT INTO ITEMS VALUES(99,1,'unauthorized')"`, 1)
	if composite {
		source = strings.Replace(source, "genpatch.Schema...", "markerSchema()...", 1)
		source = strings.ReplaceAll(source, "INSERT INTO ITEMS VALUES(", "INSERT INTO ITEMS VALUES(5,")
		source = strings.ReplaceAll(source, `"id":10`, `"tenantId":5,"id":10`)
		source = strings.ReplaceAll(source, `"id":11`, `"tenantId":5,"id":11`)
		source = strings.ReplaceAll(source, `"id":12`, `"tenantId":5,"id":12`)
		source = strings.ReplaceAll(source, `"id":20`, `"tenantId":5,"id":20`)
		source = strings.ReplaceAll(source, `"id":999`, `"tenantId":5,"id":999`)
		source = strings.ReplaceAll(source, `"id":99`, `"tenantId":5,"id":99`)
		source = strings.ReplaceAll(source, `{"name":"inserted"}`, `{"tenantId":5,"id":30,"name":"inserted"}`)

		source = strings.Replace(source, " // Init authors advancement", `
 // An incomplete tuple cannot select a row with the same local key.
 _,err=invoke("{\"Data\":[{\"id\":1,\"version\":0,\"Items\":[{\"id\":11,\"shouldDelete\":true}]}]}")
 if err==nil{t.Fatal("incomplete composite delete accepted")}
 if err=db.ExecStatements(ctx,"INSERT INTO ITEMS VALUES(6,11,1,'other tenant')");err!=nil{t.Fatal(err)}
 _,err=invoke("{\"Data\":[{\"id\":1,\"version\":0,\"Items\":[{\"tenantId\":5,\"id\":11,\"shouldDelete\":true}]}]}")
 if err!=nil{t.Fatal(err)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT TENANT_ID,NAME FROM ITEMS WHERE ID=11"},[]struct{TenantId int;Name string}{{6,"other tenant"}})
 // Init authors advancement`, 1)
		source += `
func markerSchema()[]string{result:=append([]string{},genpatch.Schema...);for i,statement:=range result{statement=strings.Replace(statement,"ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID","TENANT_ID INTEGER NOT NULL DEFAULT 5, ID INTEGER NOT NULL, ORDER_ID",1);if strings.HasPrefix(statement,"CREATE TABLE ITEMS"){statement=strings.TrimSuffix(statement,")")+", PRIMARY KEY(TENANT_ID,ID))"};statement=strings.Replace(statement,"INSERT INTO ITEMS VALUES(10,","INSERT INTO ITEMS VALUES(5,10,",1);result[i]=statement};return result}
`
	}
	// Setter fixture still uses time; keep that import.
	source = strings.Replace(source, "\"errors\"", "\"errors\"\n\"time\"", 1)
	genpatch.Run(t, root, dir, source)
}

const mutationMarkersRuntime = `
 actual,err:=invoke(` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":10,"name":"updated"},{"id":12,"shouldDelete":true},{"name":"inserted"}]}]}` + "`" + `)
 if err!=nil{t.Fatal(err)};_ = actual;if !observedDeleteParent{t.Fatal("delete flag or typed parent missing in child lifecycle")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ORDER_ID=1 AND ID<>99 ORDER BY NAME"},[]struct{Name string}{{"inserted"},{"preserved"},{"updated"}})
 for _,body:=range []string{
 ` + "`" + `{"Data":[{"id":1,"version":0}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"version":0,"Items":[]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":11,"shouldDelete":false}]}]}` + "`" + `,
 }{if _,err=invoke(body);err!=nil{t.Fatal(err)}}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ORDER_ID=1 AND ID<>99 ORDER BY NAME"},[]struct{Name string}{{"inserted"},{"preserved"},{"updated"}})
 for _,body:=range []string{
 ` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"shouldDelete":true}]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":999,"shouldDelete":true}]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":99,"shouldDelete":true}]}]}` + "`" + `,
 }{if _,err=invoke(body);err==nil{t.Fatal("unsafe delete accepted",body)}}
 for _,body:=range []string{
 ` + "`" + `{"Data":[{"id":1,"version":1,"Items":[{"id":10,"name":"forbidden"}]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"version":1,"start":"2027-01-01T00:00:00Z","Items":[{"name":null}]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"id":1,"Items":[{"id":10,"name":"forbidden"}]}]}` + "`" + `,
 }{customValidationCalls=0;_,err=invoke(body);if customValidationCalls!=0{t.Fatal("business validation ran before conflict")};var conflict *xhandler.Conflict;if !errors.As(err,&conflict){t.Fatalf("expected typed conflict: %v",err)}}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ORDER_ID=1 AND ID<>99 ORDER BY NAME"},[]struct{Name string}{{"inserted"},{"preserved"},{"updated"}})
 remapDelete=true
 _,err=invoke(` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":999,"shouldDelete":true}]}]}` + "`" + `)
 remapDelete=false
 if err==nil{t.Fatal("unknown original identity was resolved into another deletion")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=11"},[]struct{Name string}{{"preserved"}})
 // Matching an authorized child loaded for another supplied parent must fail.
 _,err=invoke(` + "`" + `{"Data":[{"id":1,"version":0,"Items":[{"id":20,"shouldDelete":true}]},{"id":2,"version":0}]}` + "`" + `)
 if err==nil{t.Fatal("cross-parent deletion accepted")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=20"},[]struct{Name string}{{"other child"}})
 // Omitted children do not become deletes, even when a parent is marked.
 _,err=invoke(` + "`" + `{"Data":[{"id":2,"removeMe":true}]}` + "`" + `)
 if err==nil{t.Fatal("parent with omitted constrained child unexpectedly deleted")}
 _,err=invoke(` + "`" + `{"Data":[{"id":2,"removeMe":true,"Items":[{"id":20}]}]}` + "`" + `)
 if err==nil{t.Fatal("unflagged supplied child under deleted parent accepted")}
 _,err=invoke(` + "`" + `{"Data":[{"id":2,"removeMe":true,"Items":[{"id":20,"shouldDelete":true}]}]}` + "`" + `)
 if err!=nil{t.Fatalf("explicit nested delete order: %v",err)}
 var count int;if err=db.DB.QueryRow("SELECT COUNT(*) FROM ORDERS WHERE ID=2").Scan(&count);err!=nil||count!=0{t.Fatal("nested deletion did not execute",err)}
 // Init authors advancement independently; comparison still uses original zero.
 var beforeAdvance int;if err=db.DB.QueryRow("SELECT VERSION FROM ORDERS WHERE ID=1").Scan(&beforeAdvance);err!=nil||beforeAdvance!=0{t.Fatalf("unexpected token before Init: version=%d err=%v",beforeAdvance,err)}
 advanceVersion=true
 _,err=invoke(` + "`" + `{"Data":[{"id":1,"version":0}]}` + "`" + `);if err!=nil{t.Fatalf("captured expected token lost during Init: %v",err)}
 advanceVersion=false
 var version int;if err=db.DB.QueryRow("SELECT VERSION FROM ORDERS WHERE ID=1").Scan(&version);err!=nil||version!=1{t.Fatal("authored advancement missing",err)}
 _,err=invoke(` + "`" + `{"Data":[{"id":1,"version":1}]}` + "`" + `);if err!=nil{t.Fatal(err)}
 if err=db.DB.QueryRow("SELECT VERSION FROM ORDERS WHERE ID=1").Scan(&version);err!=nil||version!=1{t.Fatal("framework automatically advanced token",err)}

}
`
