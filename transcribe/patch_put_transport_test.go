package transcribe

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestWriterTransportCompatibilityRetainsPolicyBoundaries(t *testing.T) {
	for _, tc := range []struct {
		operation, method string
		allowed           bool
	}{
		{"patch", "PATCH", true}, {"patch", "PUT", true}, {"patch", "put", true},
		{"post", "PATCH", true}, {"put", "PUT", true},
		{"put", "PATCH", false}, {"post", "PUT", false}, {"patch", "POST", false},
		{"patch", "GET", false}, {"patch", "DELETE", false}, {"put", "DELETE", false},
	} {
		t.Run(tc.operation+"/"+tc.method, func(t *testing.T) {
			if err := validateWriterRoute(tc.operation, tc.method, nil); (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v error=%v", tc.allowed, err)
			}
		})
	}
}

func TestGeneratorSharedPatchPutTransportsPreserveUpsert(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	dql := strings.Replace(genpatch.LifecycleDQL, "$route('/orders','PATCH')", "$route('/orders','PATCH','PUT')", 1)
	dql = strings.Replace(dql, "SELECT o.*,", "#define($_ = $Method<string>(http_request/method))\nSELECT o.*,", 1)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: dql, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Result.Plan.Settings.Mutation != "patch" || len(got.Result.Plan.Routes) != 2 || got.Result.Plan.Routes[0].Method != "PATCH" || got.Result.Plan.Routes[1].Method != "PUT" {
		t.Fatalf("policy or routes changed: %+v", got.Result.Plan)
	}
	pkgDir := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	genpatch.ObserveHooks(t, pkgDir)
	runtime := genpatch.RuntimeSource
	runtime = strings.Replace(runtime, "var lookupRead bool", "var lookupRead bool\nvar expectedTransport string", 1)
	runtime = strings.Replace(runtime, "func(input *OrdersInput)Init(context.Context)error{", "func(input *OrdersInput)Init(context.Context)error{\n if input.Method!=expectedTransport{return fmt.Errorf(\"request method changed: %s\",input.Method)}", 1)
	runtime = strings.Replace(runtime, "func TestGeneratedPatchRuntime(t *testing.T){", "func TestGeneratedPatchRuntime(t *testing.T){\n for _,transport:=range []string{\"PATCH\",\"PUT\"}{t.Run(transport,func(t *testing.T){expectedTransport=transport", 1)
	runtime = strings.Replace(runtime, "FieldByName(\"Contract\")", "FieldByName(\"Contract1\")", 1)
	// Resolve the second actual generated contract, retaining its policy metadata.
	needle := "resources:=resource.New();"
	runtime = strings.Replace(runtime, needle, `second,ok:=holder.FieldByName("Contract2");if !ok{t.Fatal("missing PUT holder")}
 secondTag,present,err:=dtag.ParseComponent(second.Tag);if err!=nil||!present{t.Fatal(err)}
 secondSource:=&bootstrap.RouteSource{HolderType:"OrdersComponent",FieldName:second.Name,PackageName:"orders",PackagePath:holder.PkgPath(),Tag:secondTag,InputType:"OrdersInput",OutputType:"OrdersOutput"}
 secondComponent,err:=secondSource.Resolve(reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}));if err!=nil{t.Fatal(err)}
 if component.Settings.Mutation!="patch"||secondComponent.Settings.Mutation!="patch"{t.Fatal("transport changed mutation policy")}
 component.Routes=append(component.Routes,secondComponent.Routes...)
 `+needle, 1)
	runtime = strings.Replace(runtime, "httptest.NewRequest(\"PATCH\",", "httptest.NewRequest(transport,", 1)
	runtime = strings.Replace(runtime, "rt.ExecuteRoute(ctx,\"PATCH\",", "rt.ExecuteRoute(ctx,transport,", 1)
	needle = "\n}\n\nfunc TestGeneratedSettersDrivePresence"
	rollback := `
 if err:=db.ExecStatements(ctx,"CREATE TRIGGER reject_item BEFORE UPDATE ON ITEMS WHEN NEW.NAME='reject' BEGIN SELECT RAISE(ABORT,'late item failure'); END");err!=nil{t.Fatal(err)}
 _,err=invoke("{\"Data\":[{\"id\":1,\"name\":\"must rollback\",\"Items\":[{\"id\":10,\"name\":\"reject\"}]}]}")
 if err==nil{t.Fatal("late physical failure accepted")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS WHERE ID=1"},[]struct{Name string}{{"before"}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=10"},[]struct{Name string}{{"updated"}})
 })}
}

func TestGeneratedSettersDrivePresence`
	if !strings.Contains(runtime, needle) {
		t.Fatal("runtime fixture boundary changed")
	}
	runtime = strings.Replace(runtime, needle, rollback, 1)
	genpatch.Run(t, root, pkgDir, runtime, "-race", "-v")
}
