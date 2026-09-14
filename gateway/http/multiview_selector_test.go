package http

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	mcpinvocation "github.com/viant/datly/mcp/invocation"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/generate"
)

type multiviewInput struct {
	AlternativeFields []string `parameter:"Fields,kind=query,in=alternative_fields"`
	TagFields         []string `parameter:"Fields,kind=query,in=tag_fields"`
	Fields            []string `parameter:"Fields,kind=query,in=_fields"`
	ProductFields     []string `parameter:"Fields,kind=query,in=product_fields"`
	Limit             int      `parameter:"Limit,kind=query,in=_limit"`
	Page              int      `parameter:"Page,kind=query,in=_page"`
	OrderBy           string   `parameter:"OrderBy,kind=query,in=_orderby"`
	Criteria          string   `parameter:"Criteria,kind=query,in=_criteria"`
	ProductLimit      int      `parameter:"Limit,kind=query,in=product_limit"`
	ProductPage       int      `parameter:"Page,kind=query,in=product_page"`
	ProductOrderBy    string   `parameter:"OrderBy,kind=query,in=product_orderby"`
	ProductCriteria   string   `parameter:"Criteria,kind=query,in=product_criteria"`
}
type multiviewTag struct {
	ID        int    `sqlx:"id" json:"id"`
	ProductID int    `sqlx:"product_id" json:"productId"`
	Name      string `sqlx:"name" json:"label"`
}
type multiviewAlternative struct {
	ID          int    `sqlx:"id" json:"id"`
	InventoryID int    `sqlx:"inventory_id" json:"inventoryId"`
	Name        string `sqlx:"name" json:"title"`
}
type multiviewProduct struct {
	Tags []*multiviewTag `view:"tags,table=tags,selectorProjection=true" on:"ID:id=ProductID:product_id" json:"tags"`

	ID          int     `sqlx:"id" json:"id"`
	InventoryID int     `sqlx:"inventory_id" json:"inventoryId"`
	Name        string  `sqlx:"name" json:"display_name"`
	Price       int     `sqlx:"price" json:"price"`
	Note        *string `sqlx:"note" json:"note"`
}
type MultiviewIdentity struct {
	ID int `sqlx:"id" json:"id"`
}
type multiviewRow struct {
	MultiviewIdentity
	Alternative []*multiviewAlternative `view:"alternatives,table=products,selectorProjection=true" on:"ID:id=InventoryID:inventory_id" json:"alternative"`
	Name        string                  `sqlx:"name" json:"name"`
	Product     []*multiviewProduct     `view:"products,table=products" on:"ID:id=InventoryID:inventory_id" json:"product"`
}
type multiviewOutput struct {
	Rows []*multiviewRow `json:"rows"`
}

const multiviewSource = `#setting($_ = $route('/inventory','GET'))
#define($_ = $Fields<[]string>(query/alternative_fields).Optional().QuerySelector('alternatives'))
#define($_ = $Fields<[]string>(query/tag_fields).Optional().QuerySelector('tags'))
#define($_ = $Fields<[]string>(query/_fields).Optional().QuerySelector('inventory'))
#define($_ = $Fields<[]string>(query/product_fields).Optional().QuerySelector('products'))
#define($_ = $Limit<int>(query/_limit).Optional().QuerySelector('inventory'))
#define($_ = $Page<int>(query/_page).Optional().QuerySelector('inventory'))
#define($_ = $OrderBy<string>(query/_orderby).Optional().QuerySelector('inventory'))
#define($_ = $Criteria<string>(query/_criteria).Optional().QuerySelector('inventory'))
#define($_ = $Limit<int>(query/product_limit).Optional().QuerySelector('products'))
#define($_ = $Page<int>(query/product_page).Optional().QuerySelector('products'))
#define($_ = $OrderBy<string>(query/product_orderby).Optional().QuerySelector('products'))
#define($_ = $Criteria<string>(query/product_criteria).Optional().QuerySelector('products'))
#define($_ = $Rows<?>(output/view))
SELECT id,name FROM inventory`

// Shares the SQLite owner and real DQL/bootstrap/HTTP path for linked and
// generated inputs and both direct and enveloped typed outputs.
func multiViewHandler(t *testing.T, direct, generated bool, extra ...string) *Handler {
	t.Helper()
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE inventory(id INTEGER,name TEXT)", "CREATE TABLE products(id INTEGER,inventory_id INTEGER,name TEXT,price INTEGER,note TEXT)", "CREATE TABLE tags(id INTEGER,product_id INTEGER,name TEXT)", "INSERT INTO tags VALUES(100,20,'tag-two')", "INSERT INTO inventory VALUES(1,'inventory')", "INSERT INTO products VALUES(10,1,'first',5,NULL),(20,1,'second',0,NULL)"))
	source := multiviewSource
	if direct {
		source = strings.Replace(source, "#define($_ = $Rows<?>(output/view))\n", "", 1)
	}
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/inventory", Name: "Inventory", Text: source})
	require.NoError(t, err)
	compiled.Component.RootView.Selector = &spec.Selector{AllowFields: true, AllowLimit: true, AllowPage: true, AllowOrderBy: true, AllowCriteria: true, Filterable: []spec.FieldPath{"id", "price", "name"}, Orderable: []spec.FieldPath{"id"}, DefaultOrder: "id ASC"}
	inputType := reflect.TypeFor[multiviewInput]()
	if generated {
		inputType, err = generate.New(generate.Input{Component: compiled.Component}).RuntimeInputType()
		require.NoError(t, err)
		for _, name := range []string{"Fields", "ProductFields", "Limit", "ProductLimit", "Page", "ProductPage"} {
			field, ok := inputType.FieldByName(name)
			require.True(t, ok, name)
			require.True(t, field.IsExported(), name)
		}
	}
	outputType := reflect.TypeFor[multiviewOutput]()
	holder := "Rows"
	if direct {
		outputType = reflect.TypeFor[[]*multiviewRow]()
		holder = ""
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: inputType, OutputType: outputType, DirectViewField: holder})
	require.NoError(t, err)
	artifact.Reader.Root.View.Relations[1].Of.View.Spec.Selector = &spec.Selector{AllowFields: true, AllowLimit: true, AllowPage: true, AllowOrderBy: true, AllowCriteria: true, Filterable: []spec.FieldPath{"id", "price", "name"}, Orderable: []spec.FieldPath{"id"}, DefaultOrder: "id DESC"}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: outputType, Reader: reader}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
	require.NoError(t, db.ExecStatements(ctx, extra...))
	return NewHandler(rt, nil, "test")
}

func TestHTTPMultiViewFieldSelection(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, generated := range []bool{false, true} {
			for _, tc := range []struct {
				name, query, want string
				drop              bool
			}{
				{"root only", "_fields=id&_fields=name", `[{"id":1,"name":"inventory"}]`, true},
				{"child cannot enable omitted relation", "_fields=name&product_fields=name", `[{"name":"inventory"}]`, true},
				{"child fields and limit", "_fields=id&_fields=name&_fields=Product&product_fields=name&product_limit=1", `[{"id":1,"name":"inventory","product":[{"display_name":"second"}]}]`, false},
				{"explicit zero child limit", "_fields=Product&product_fields=name&product_limit=0", `[{"product":[{"display_name":"second"},{"display_name":"first"}]}]`, false},
				{"selected zero and nil", "_fields=Product&product_fields=price&product_fields=note&product_limit=1", `[{"product":[{"price":0,"note":null}]}]`, false},
				{"selected internal key", "_fields=Product&product_fields=inventory_id&product_limit=1", `[{"product":[{"inventoryId":1}]}]`, false},
				{"empty relation stays selected", "_fields=Product&product_fields=name&product_criteria=" + url.QueryEscape("id = 999"), `[{"product":null}]`, false},
			} {
				t.Run(fmt.Sprintf("%s/direct=%v/generated=%v", tc.name, direct, generated), func(t *testing.T) {
					var extra []string
					if tc.drop {
						extra = append(extra, "DROP TABLE products")
					}
					h := multiViewHandler(t, direct, generated, extra...)
					response := httptest.NewRecorder()
					h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+tc.query, nil))
					require.Equal(t, 200, response.Code, response.Body.String())
					want := tc.want
					if !direct {
						want = `{"rows":` + want + `}`
					}
					require.JSONEq(t, want, response.Body.String())
				})
			}
		}
	}
}

func TestHTTPMultiViewIndependentControls(t *testing.T) {
	h := multiViewHandler(t, false, true, "INSERT INTO inventory VALUES(2,'two'),(3,'three')", "INSERT INTO products VALUES(30,2,'two-first',4,NULL),(40,2,'two-second',8,NULL),(50,3,'three',12,NULL)")
	query := url.Values{"_fields": {"name", "Product"}, "_criteria": {"id >= 2"}, "_orderby": {"id DESC"}, "_limit": {"1"}, "_page": {"2"}, "product_fields": {"name"}, "product_criteria": {"price >= 4"}, "product_orderby": {"id ASC"}, "product_limit": {"1"}, "product_page": {"2"}}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+query.Encode(), nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"rows":[{"name":"two","product":[{"display_name":"two-second"}]}]}`, response.Body.String())
}

func TestHTTPMultiViewConcurrentSelection(t *testing.T) {
	h := multiViewHandler(t, false, true)
	var wg sync.WaitGroup
	failures := make(chan string, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			query, want := "_fields=name", `{"rows":[{"name":"inventory"}]}`
			if i%2 == 1 {
				query = "_fields=Product&product_fields=price&product_limit=1"
				want = `{"rows":[{"product":[{"price":0}]}]}`
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+query, nil))
			if response.Code != 200 || response.Body.String() != want {
				failures <- response.Body.String()
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestHTTPMultiViewSiblingAndNestedSelection(t *testing.T) {
	h := multiViewHandler(t, false, true)
	query := url.Values{"_fields": {"Product", "Alternative"}, "product_fields": {"name", "Tags"}, "product_limit": {"1"}, "tag_fields": {"name"}, "alternative_fields": {"id"}}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+query.Encode(), nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"rows":[{"alternative":[{"id":10},{"id":20}],"product":[{"display_name":"second","tags":[{"label":"tag-two"}]}]}]}`, response.Body.String())
}

func TestHTTPMultiViewOmittedNestedViewDoesNotQuery(t *testing.T) {
	h := multiViewHandler(t, false, true, "DROP TABLE tags")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?_fields=Product&product_fields=name&product_limit=1&tag_fields=name", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"rows":[{"product":[{"display_name":"second"}]}]}`, response.Body.String())
}

func TestHTTPMultiViewRejectsTwoArgumentSelector(t *testing.T) {
	_, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{Scope: "example.com/inventory", Name: "Invalid", Text: strings.Replace(multiviewSource, "QuerySelector('products')", "QuerySelector('products','fields')", 1)})
	require.Error(t, err)
}

func TestMCPMultiViewSelectedPayloadRetainsTypedResult(t *testing.T) {
	h := multiViewHandler(t, false, true)
	request := httptest.NewRequest("GET", "/inventory?_fields=Product&product_fields=price&product_limit=1", nil)
	scope, err := requestprovider.New(request)
	require.NoError(t, err)
	defer scope.Close()
	invoker := mcpinvocation.New(mcpinvocation.Config{Invoker: h.runtime})
	execution, protocolErr := invoker.Execute(context.Background(), mcpinvocation.Request{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/inventory", Name: "Inventory"}, Route: spec.RouteRef{Method: "GET", Path: "/inventory"}}, Scope: scope, Method: "tools/call", URI: "inventory"})
	require.Nil(t, protocolErr)
	require.NoError(t, execution.Error())
	output, ok := execution.Value().(*multiviewOutput)
	require.True(t, ok)
	require.Equal(t, 1, output.Rows[0].Product[0].InventoryID)
	payload, err := execution.Payload()
	require.NoError(t, err)
	require.JSONEq(t, `{"rows":[{"product":[{"price":0}]}]}`, string(payload))
}

func TestHTTPMultiViewTypedCriteriaValueIsBound(t *testing.T) {
	h := multiViewHandler(t, false, true)
	query := url.Values{"_fields": {"Product"}, "product_fields": {"name"}, "product_criteria": {"name = 'second'' OR 1=1 --'"}}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+query.Encode(), nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"rows":[{"product":null}]}`, response.Body.String())
	query.Set("product_criteria", "name = 'second'")
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+query.Encode(), nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"rows":[{"product":[{"display_name":"second"}]}]}`, response.Body.String())
}
