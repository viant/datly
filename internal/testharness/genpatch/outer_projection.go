package genpatch

import (
	_ "embed"
	"strings"
)

//go:embed outer_projection_runtime.go.txt
var OuterProjectionRuntime string

var OuterProjectionDecoys = []string{
	`INSERT INTO ORDERS SELECT 2,KIND_ID,'hidden',START,END FROM ORDERS WHERE ID=1`,
	`INSERT INTO ITEMS VALUES(11,1,'hidden child'),(12,2,'other parent')`,
}

// OuterProjectionDQL keeps the DB sources and their restrictions fixed while
// changing only the public named-view projection.
func OuterProjectionDQL(module, operation, root, child string, split bool) string {
	header := "#package('api/orders')\n#setting($_ = $route('/orders','" + strings.ToUpper(operation) + "'))\n"
	if split {
		header += `#import('requests','` + module + `/requests')
#import('responses','` + module + `/responses')
#import('rows','` + module + `/entities')
#import('children','` + module + `/items')
#setting($_ = $input_type('requests.OrdersInput'))
#setting($_ = $output_type('responses.OrdersOutput'))
`
	}
	projection := root
	if child != "" {
		projection += ", " + child
	}
	projection += ", kind.*"
	if split {
		projection += ", type(orders,'rows.Order'), dest(orders,'order.go'), type(items,'children.Item'), dest(items,'item.go')"
	}
	return header + "SELECT " + projection + `
FROM (SELECT o.* FROM ORDERS o WHERE o.ID=1) orders
LEFT JOIN (SELECT i.* FROM ITEMS i WHERE i.ID=10) items ON items.ORDER_ID=orders.ID
LEFT JOIN (SELECT k.* FROM (ORDER_KINDS) k WHERE k.ID=7) kind ON kind.ID=orders.KIND_ID AND 1=1
WHERE orders.ID < 2
`
}

const OuterProjectionWriterImports = `
 "net/http/httptest"
 "strings"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 druntime "github.com/viant/datly/runtime"
 mutationhandler "github.com/viant/datly/runtime/handler/mutation"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
`
const OuterProjectionWriterTest = `
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 handler:=mutationhandler.New[OrdersInput,OrdersOutput](NewOrdersHandler())
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 request:=httptest.NewRequest("PATCH","/orders",strings.NewReader(BODY));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 if _,err=rt.ExecuteRoute(ctx,"PATCH","/orders",scope);err!=nil{t.Fatal(err)}
 if lifecycleCalls==0{t.Fatal("authored Lifecycle was not invoked")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS WHERE ID=1"},[]struct{Name string}{{WANT_ROOT}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=CHILD_ID"},[]struct{Name string}{{WANT_CHILD}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=11"},[]struct{Name string}{{"hidden child"}})
`
