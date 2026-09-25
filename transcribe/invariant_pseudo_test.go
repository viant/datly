package transcribe

import (
	"bytes"
	"context"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	tcompile "github.com/viant/datly/transcribe/compile"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/golang"
)

func TestInvariantPseudoNamedViewsSQLiteAndGo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		`CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY, VENDOR_ID INTEGER NOT NULL, WINDOW_START INTEGER NOT NULL, WINDOW_END INTEGER NOT NULL)`,
		`CREATE TABLE VENDOR(ID INTEGER PRIMARY KEY)`,
		`INSERT INTO ORDERS VALUES(1,2,3,9)`, `INSERT INTO VENDOR VALUES(2)`,
	); err != nil {
		t.Fatal(err)
	}
	const SQL = `SELECT orders.*, vendor.*, invariant(orders.WINDOW_START, 'DeliveryWindow'), invariant(orders.WINDOW_END, 'DeliveryWindow') FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT v.* FROM (VENDOR) v) vendor ON vendor.ID=orders.VENDOR_ID`
	graph, err := tcompile.NewReader().Compile(tcompile.ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	vendor := graph.Relations[0].View
	if !vendor.Auxiliary || !strings.Contains(vendor.Source.SQL, "(VENDOR)") {
		t.Fatalf("auxiliary source changed: %s", vendor.Source.SQL)
	}
	for _, v := range []*spec.View{graph, vendor} {
		if strings.Contains(strings.ToLower(v.Source.SQL), "invariant(") {
			t.Fatalf("annotation reached executable SQL: %s", v.Source.SQL)
		}
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: graph.Source.SQL}, []struct{ ID, VENDOR_ID, WINDOW_START, WINDOW_END int }{{1, 2, 3, 9}})
	h.AssertQuery(t, ctx, sqlite.Query{SQL: vendor.Source.SQL}, []struct{ ID int }{{2}})
	// Discover and generate the complete named-view graph.
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Orders", Scope: "example.com/generated/orders", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#setting($_ = $route('/orders','GET'))\n" + SQL})
	if err != nil {
		t.Fatal(err)
	}
	view := compiled.Component.RootView
	if len(view.Relations) != 1 || !view.Relations[0].View.Auxiliary || len(view.Relations[0].View.Columns) != 1 {
		t.Fatalf("full graph discovery lost auxiliary view: %+v", view.Relations)
	}
	identity, err := view.Identity()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{}).Write(t, root)
	dir := filepath.Join(root, "orders")
	generated, err := gen.New(gen.Input{Component: compiled.Component, SetMarkerViews: map[string]bool{identity: true}}).Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	var row gen.ViewPlan
	for _, v := range generated.Plan.Views {
		if v.Identity == identity {
			row = v
		}
	}
	for _, name := range []string{"WindowStart", "WindowEnd"} {
		field, ok := row.Field(name)
		if !ok || reflect.StructTag(field.Tag).Get("invariant") != "DeliveryWindow" {
			t.Fatalf("generated field %s=%+v", name, field)
		}
	}
	emitted, err := os.ReadFile(filepath.Join(dir, row.Destination))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(emitted), `invariant:"DeliveryWindow"`) != 2 {
		t.Fatalf("emitted tags: %s", emitted)
	}
	path := plan.FieldPath{"Input", "Records"}
	semantic := &plan.Plan{Operation: plan.OperationPost, Input: plan.ContractRef{Path: path, Cardinality: spec.CardinalityMany}, Root: &plan.RecordPlan{Identity: identity, InputPath: path, Table: "ORDERS", Cardinality: spec.CardinalityMany, Write: plan.WritePolicy{ValuePath: path, Missing: plan.ActionInsert, Allowed: []plan.Action{plan.ActionInsert}}}}
	refined, err := newHandlerGeneration(nil, nil, Options{}).withGeneratedPresence(semantic, generated.Plan)
	if err != nil {
		t.Fatal(err)
	}
	want := []plan.InvariantGroup{{Name: "DeliveryWindow", Fields: []string{"WindowStart", "WindowEnd"}}}
	if !reflect.DeepEqual(refined.Root.Entity.Invariants, want) {
		t.Fatalf("invariant metadata=%+v", refined.Root.Entity.Invariants)
	}
	if semantic.Root.Entity != nil {
		t.Fatal("generation mutated canonical semantic plan")
	}
	asset, err := golang.EntitySupport(refined, golang.Config{Package: generated.Plan.PackageName(), Factory: "NewOrders", InputType: "InvariantInput", OutputType: "InvariantOutput", Records: []golang.RecordType{{Identity: identity, Path: path, Value: "[]*" + row.Name}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(asset.Invariants) != 1 || asset.Invariants[0].Group != "DeliveryWindow" || asset.Invariants[0].BackfillFunction == "" {
		t.Fatalf("helper metadata=%+v", asset.Invariants)
	}
	var support bytes.Buffer
	if err = format.Node(&support, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "invariant_support.go"), support.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	consumer := strings.ReplaceAll(invariantPseudoConsumer, "ROW_TYPE", row.Name)
	if err = os.WriteFile(filepath.Join(dir, "invariant_consumer_test.go"), []byte(consumer), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated invariant consumer: %v\n%s\n%s", err, output, support.Bytes())
	}
}

const invariantPseudoConsumer = `package orders
import("testing";"reflect")
type InvariantInput struct{Records []*ROW_TYPE}
type InvariantOutput struct{}
func TestAuthoredInvariantHelpers(t *testing.T){
 start,end,zero:=3,9,0
 previous:=&ROW_TYPE{WindowStart:&start,WindowEnd:&end}
 current:=&ROW_TYPE{}
 if current.HasDeliveryWindowChanges(){t.Fatal("inactive group")}
 current.SetWindowStart(&zero)
 if !current.HasDeliveryWindowChanges(){t.Fatal("explicit zero did not activate group")}
 markers:=*current.Has
 if err:=current.BackfillDeliveryWindowIfNeeded(previous,nil);err!=nil{t.Fatal(err)}
 if current.WindowStart==nil||*current.WindowStart!=0||current.WindowEnd==nil||*current.WindowEnd!=9||current.WindowEnd==previous.WindowEnd||!reflect.DeepEqual(*current.Has,markers){t.Fatalf("backfill=%+v",current)}
 null:=&ROW_TYPE{};null.SetWindowStart(nil)
 if err:=null.BackfillDeliveryWindowIfNeeded(previous,nil);err!=nil{t.Fatal(err)}
 if null.WindowStart!=nil||null.WindowEnd==nil||*null.WindowEnd!=9{t.Fatal("explicit null lost")}
 for _,name:=range []string{"WindowStart","WindowEnd"}{field,_:=reflect.TypeOf(*current).FieldByName(name);if field.Tag.Get("invariant")!="DeliveryWindow"{t.Fatalf("tag lost: %s",name)}}
}
`
