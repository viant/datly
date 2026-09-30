package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestGeneratedInferredOutputSummary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE orders(id INTEGER, tenant_id INTEGER, name TEXT)", "INSERT INTO orders VALUES (1,7,'a'),(2,7,'b'),(3,7,'c'),(4,9,'private')"))
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/summaryfixture"}).Write(t, root)
	compiler := NewCompiler()
	compiled, err := compiler.Compile(ctx, &Source{
		Name: "Orders", Scope: "github.com/viant/datly/summaryfixture/orders", Connector: "main", Types: typecatalog.NewCatalog(),
		Text: `#setting($_ = $route('/orders', 'GET'))
#set($_ = $TenantId<int>(query/tenant).Value(7))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(orderRows).Optional())
#set($_ = $Limit<int>(query/limit).QuerySelector(orderRows).Optional().Value(2))
#set($_ = $Offset<int>(query/offset).QuerySelector(orderRows).Optional())
#set($_ = $Meta<?>(output/summary) /*
SELECT CAST((COUNT(1) + COALESCE(NULLIF($View.Limit, 0), 1) - 1) / COALESCE(NULLIF($View.Limit, 0), 1) AS INTEGER) AS PAGE_COUNT,
       COUNT(1) AS RECORD_COUNT FROM ($View.NonWindowSQL) t
*/)
#set($_ = $Data<?>(output/view))
SELECT orderRows.id, orderRows.name FROM orders orderRows WHERE orderRows.tenant_id = $TenantId ORDER BY orderRows.id`,
	})
	require.NoError(t, err)
	// SQLite does not report aggregate expression types. Supply those scalar
	// hints while still discovering both result sets; the output holder is <?>.
	compiled.Component.RootView.Relations[0].View.Columns = []*spec.Column{
		{Name: "PAGE_COUNT", Type: spec.TypeRef{Name: "int"}, ExplicitType: true},
		{Name: "RECORD_COUNT", Type: spec.TypeRef{Name: "int"}, ExplicitType: true},
	}
	input, err := (&discoveryInputCompiler{component: compiled.Component, source: compiled.Source, declarations: compiled.Declarations, resolver: compiled.TypeResolver, viewBindings: compiled.ViewBindings}).compile()
	require.NoError(t, err)
	require.NoError(t, column.New(column.Connections{"main": db.DB}).Refine(ctx, compiled.Component, nil, input))
	generated, err := compiler.generateCompiled(ctx, root, compiled)
	require.NoError(t, err)
	plan := generated.Result.Plan
	for _, field := range plan.Input.Fields {
		switch field.Name {
		case "Fields":
			require.Contains(t, field.Tag, `querySelector:"view=Orders,property=fields"`)
		case "Limit":
			require.Contains(t, field.Tag, `querySelector:"view=Orders,property=limit"`)
		case "Offset":
			require.Contains(t, field.Tag, `querySelector:"view=Orders,property=offset"`)
		}
	}
	require.Equal(t, "*MetaView", plan.Output.Fields[0].Type)
	for _, view := range plan.Views {
		if view.Type == plan.RootViewType {
			for _, field := range view.Fields {
				require.NotEqual(t, "Meta", field.Name)
			}
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "generated", "summary_test.go"), []byte(generatedSummaryTest), 0600))
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

const generatedSummaryTest = `package orders

import (
 "context"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 gateway "github.com/viant/datly/gateway/http"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
)

func TestGeneratedSummaryExecution(t *testing.T) {
 ctx:=context.Background()
 db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE orders(id INTEGER, tenant_id INTEGER, name TEXT)","INSERT INTO orders VALUES (1,7,'a'),(2,7,'b'),(3,7,'c'),(4,9,'private')"))
 _,present:=reflect.TypeFor[OrdersView]().FieldByName("Meta")
 require.False(t,present)
 resources:=resource.New()
 require.NoError(t,resources.Register(DatlyResourceNamespace,DatlyResources))
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{
  Component:&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Name:"Orders"},Routes:[]*spec.Route{{Method:"GET",Path:"/orders"}}},
  InputType:reflect.TypeFor[OrdersInput](),OutputType:reflect.TypeFor[OrdersOutput](),Resources:resources,
 })
 require.NoError(t,err)
 reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}})
 require.NoError(t,err)
 registered,err:=artifact.Registration(druntime.RegisteredComponent{Reader:reader})
 require.NoError(t,err)
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
 require.NoError(t,err)
 t.Cleanup(func(){require.NoError(t,rt.Shutdown(context.Background()))})
 handler:=gateway.NewHandler(rt,nil,"test")
 for _,tc:=range []struct{name,query string;rows,records,pages int}{
  {"populated","?limit=2",2,3,2},
  {"empty","?tenant=99&limit=2",0,0,0},
  {"projected","?fields=id&limit=2",2,3,2},
  {"last page","?offset=2&limit=2",1,3,2},
  {"past last page","?offset=9&limit=2",0,3,2},
  {"different limit","?limit=1",1,3,3},
 } {
  t.Run(tc.name,func(t *testing.T){
   res:=httptest.NewRecorder()
   handler.ServeHTTP(res,httptest.NewRequest("GET","/orders"+tc.query,nil))
   require.Equal(t,200,res.Code,res.Body.String())
   var actual struct{Meta struct{PageCount int;RecordCount int};Data []map[string]any}
   require.NoError(t,json.Unmarshal(res.Body.Bytes(),&actual))
   require.Len(t,actual.Data,tc.rows)
   require.Equal(t,tc.records,actual.Meta.RecordCount)
   require.Equal(t,tc.pages,actual.Meta.PageCount)
   for _,row:=range actual.Data {require.NotContains(t,row,"Meta");if tc.name=="projected"{require.Len(t,row,1)}}
  })
 }
}
`
