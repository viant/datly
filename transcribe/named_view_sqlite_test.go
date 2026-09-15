package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	rcompile "github.com/viant/datly/sql/reader/compiler"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
)

func TestNamedViewReaderRestrictionsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	schema := []string{
		`CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY,KIND_ID INTEGER,WINDOW_START INTEGER,WINDOW_END INTEGER,ENABLED INTEGER)`,
		`CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY,ORDER_ID INTEGER,VISIBLE INTEGER)`,
		`CREATE TABLE ORDER_KINDS(ID INTEGER PRIMARY KEY,ACTIVE INTEGER)`,
		`INSERT INTO ORDERS VALUES(1,7,3,9,1),(2,8,4,10,0)`,
		`INSERT INTO ITEMS VALUES(11,1,1),(12,1,0),(13,2,1)`,
		`INSERT INTO ORDER_KINDS VALUES(7,1),(8,0)`,
	}
	if err := h.ExecStatements(ctx, schema...); err != nil {
		t.Fatal(err)
	}
	const SQL = `SELECT orders.*,items.*,kind.*,invariant(orders.WINDOW_START,'DeliveryWindow'),invariant(orders.WINDOW_END,'DeliveryWindow')
FROM (SELECT o.* FROM ORDERS o WHERE o.ENABLED=1) orders
LEFT JOIN (SELECT i.* FROM ITEMS i WHERE i.VISIBLE=1) items ON items.ORDER_ID=orders.ID
LEFT JOIN (SELECT k.* FROM (ORDER_KINDS) k WHERE k.ACTIVE=1) kind ON kind.ID=orders.KIND_ID AND 1=1`
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Orders", Connector: "main", Text: "#setting($_ = $route('/orders','GET'))\n" + SQL, ColumnRefiner: column.New(column.Connections{"main": h.DB})})
	if err != nil {
		t.Fatal(err)
	}
	rootPolicy := &spec.Selector{AllowFields: true, AllowOrderBy: true}
	compiled.Component.RootView.Selector = rootPolicy
	for _, relation := range compiled.Component.RootView.Relations {
		relation.View.Selector = rootPolicy
	}
	moduleRoot := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/namedruntime"}).Write(t, moduleRoot)
	generatedDir := filepath.Join(moduleRoot, "orders")
	generated, err := generate.New(generate.Input{Component: compiled.Component}).Generate(generatedDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Plan.Views) != 3 {
		t.Fatalf("full graph generation: %+v", generated.Plan.Views)
	}
	root := compiled.Component.RootView
	if root.Auxiliary || len(root.Relations) != 2 || root.Relations[0].View.Auxiliary || !root.Relations[1].View.Auxiliary {
		t.Fatalf("auxiliary graph=%+v", root.Relations)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: root.Source.SQL}, []struct{ ID, KIND_ID, WINDOW_START, WINDOW_END, ENABLED int }{{1, 7, 3, 9, 1}})
	h.AssertQuery(t, ctx, sqlite.Query{SQL: root.Relations[0].View.Source.SQL}, []struct{ ID, ORDER_ID, VISIBLE int }{{11, 1, 1}, {13, 2, 1}})
	h.AssertQuery(t, ctx, sqlite.Query{SQL: root.Relations[1].View.Source.SQL}, []struct{ ID, ACTIVE int }{{7, 1}})
	t.Run("reader runtime nested wildcard", func(t *testing.T) {
		type item struct{ ID, ORDER_ID, VISIBLE int }
		type kind struct{ ID, ACTIVE int }
		type order struct {
			ID, KIND_ID, WINDOW_START, WINDOW_END, ENABLED int
			Items                                          []*item
			Kind                                           *kind
		}
		type output struct{ Rows []*order }
		plan, err := rcompile.Compile(rcompile.Input{Component: compiled.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
		if err != nil {
			t.Fatal(err)
		}
		execution, err := reader.NewExecution(reader.Config{Component: compiled.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
		if err != nil {
			t.Fatal(err)
		}
		actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows := actual.(*output).Rows
		if len(rows) != 1 || rows[0].ID != 1 || len(rows[0].Items) != 1 || rows[0].Items[0].ID != 11 || rows[0].Kind == nil || rows[0].Kind.ID != 7 {
			t.Fatalf("named-view restrictions or auxiliary read lost: %+v", rows)
		}
	})
	t.Run("generated tagged rows", func(t *testing.T) {
		schemaJSON, err := json.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		componentJSON, err := json.Marshal(compiled.Component)
		if err != nil {
			t.Fatal(err)
		}
		consumer := strings.NewReplacer("ROW_TYPE", generated.Plan.RootViewType, "SCHEMA_JSON", strconv.Quote(string(schemaJSON)), "COMPONENT_JSON", strconv.Quote(string(componentJSON))).Replace(namedGeneratedReaderConsumer)
		if err := os.WriteFile(filepath.Join(generatedDir, "reader_consumer_test.go"), []byte(consumer), 0644); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("go", "test", "-mod=mod", "./...")
		command.Dir = moduleRoot
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("generated tagged reader: %v\n%s", err, output)
		}
	})

}

const namedGeneratedReaderConsumer = `package orders
import (
 "context"
 "encoding/json"
 "reflect"
 "testing"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
 "github.com/viant/datly/sql/reader/compiler"
 xhandler "github.com/viant/xdatly/handler"
 xstate "github.com/viant/xdatly/state"
)
type selectedViews struct{values xstate.Selectors}
func (s selectedViews)Bind(context.Context,any)error{return nil}
func (s selectedViews)Lookup(_ context.Context,key xhandler.ValueKey)(any,bool,error){return s.values,key==xhandler.SelectorsKey,nil}
func TestGeneratedNamedGraphReader(t *testing.T){
 ctx:=context.Background();h:=sqlite.New(t)
 var schema []string;if err:=json.Unmarshal([]byte(SCHEMA_JSON),&schema);err!=nil{t.Fatal(err)}
 if err:=h.ExecStatements(ctx,schema...);err!=nil{t.Fatal(err)}
 var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
 type output struct{Rows []*ROW_TYPE}
 plan,err:=compiler.Compile(compiler.Input{Component:&component,InputType:reflect.TypeFor[struct{}](),OutputType:reflect.TypeFor[output](),DirectViewField:"Rows"});if err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:&component,InputType:reflect.TypeFor[struct{}](),OutputType:reflect.TypeFor[output](),Plan:plan,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 number:=func(value any)int64{v:=reflect.ValueOf(value);if v.Kind()==reflect.Pointer{if v.IsNil(){return 0};v=v.Elem()};return v.Int()}
 for _,mode:=range []string{"all","items only","omit relations"}{t.Run(mode,func(t *testing.T){
  var selectors xstate.Selectors
  switch mode{
  case "items only":selectors=xstate.Selectors{&xstate.NamedSelector{Name:"orders",Selector:xstate.Selector{Fields:[]string{"Items"}}},&xstate.NamedSelector{Name:"items",Selector:xstate.Selector{Fields:[]string{"ID"}}}}
  case "omit relations":selectors=xstate.Selectors{&xstate.NamedSelector{Name:"orders",Selector:xstate.Selector{Fields:[]string{"WINDOW_START"}}}}
  }
  result,err:=execution.ReadResult(ctx,&struct{}{},selectedViews{selectors},nil);if err!=nil{t.Fatal(err)}
  rows:=result.Data.(*output).Rows;if len(rows)!=1{t.Fatalf("rows=%+v",rows)}
  row:=rows[0]
  if mode=="omit relations"{if row.Items!=nil||row.Kind!=nil||number(row.WindowStart)!=3{t.Fatalf("unselected relations leaked: %+v",row)};return}
  if number(row.Id)!=1||len(row.Items)!=1||number(row.Items[0].Id)!=11{t.Fatalf("graph/matching keys lost: %+v",row)}
  if mode=="items only"{if row.Kind!=nil||number(row.Items[0].OrderId)!=1{t.Fatalf("child projection/matching lost: %+v",row)};return}
  if row.Kind==nil||number(row.Kind.Id)!=7{t.Fatalf("auxiliary view lost: %+v",row)}
 })}
}
`
