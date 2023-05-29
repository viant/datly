package doc

import (
	"context"
	"fmt"
	"github.com/viant/datly"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/toolbox"
	"log"
	"reflect"
)

type Product struct {
	Id       int
	Name     string
	VendorId int
}

func (p *Product) OnFetch(ctx context.Context) error {
	fmt.Println("breakpoint here")
	return nil
}

func (p *Product) Init() {
	fmt.Println("breakpoint here")
}

func (p *Product) Validate() bool {
	fmt.Println("breakpoint here")
	return true
}

type Validation struct {
	IsValid bool
}

//Example_RuleExecution show how to programmatically execute executor rule
func Example_ExecRuleDebugging() {
	//Uncomment various additional debugging and troubleshuting
	// expand.SetPanicOnError(false)
	// read.ShowSQL(true)
	// update.ShowSQL(true)
	// insert.ShowSQL(true)

	ctx := context.Background()
	service := datly.New(datly.NewConfig())
	ruleURL := "yyyyyyy/Datly/routes/dev/product.yaml"
	err := service.LoadRoute(ctx, ruleURL,
		view.NewPackagedType("domain", "Product", reflect.TypeOf(Product{})),
		view.NewPackagedType("domain", "Validation", reflect.TypeOf(Validation{})),
	)
	if err == nil {
		err = service.Init(ctx)
	}
	httpRequest, err := service.NewHttpRequest("PUT", "http://127.0.0.1:8080/v1/api/dev",
		&jwt.Claims{
			Email:  "dev@viantinc.com",
			UserID: 111,
		}, []byte(`{"Name":"IPad"}`))

	if err != nil {
		log.Fatal(err)
	}
	routeRes, _ := service.Routes()
	route := routeRes.Routes[0] //make sure you are using correct route
	err = service.Exec(ctx, "product", datly.WithExecHttpRequest(ctx, route, httpRequest))
	if err != nil {
		log.Fatal(err)
	}
}

//Example_ReadRuleExecution show how to programmatically execute read rule
func Example_ReadRuleDebugging() {
	//Uncomment various additional debugging and troubleshuting
	// expand.SetPanicOnError(false)
	// read.ShowSQL(true)

	ctx := context.Background()
	service := datly.New(datly.NewConfig())
	ruleURL := "yyyyyyy/Datly/routes/dev/product_get.yaml"
	err := service.LoadRoute(ctx, ruleURL,
		view.NewPackagedType("domain", "Product", reflect.TypeOf(Product{})),
	)
	//note that product has to have OnFetch(ctx context.Context) error with breakpoint for go customization
	if err == nil {
		err = service.Init(ctx)
	}
	httpRequest, err := service.NewHttpRequest("GET", "http://127.0.0.1:8080/v1/api/dev",
		&jwt.Claims{
			Email:  "dev@viantinc.com",
			UserID: 111,
		}, nil)
	if err != nil {
		log.Fatal(err)
	}
	routeRes, _ := service.Routes()
	route := routeRes.Routes[0] //make sure you are using correct route
	var products []*Product
	err = service.Read(ctx, "product_get", &products, datly.WithReadHttpRequest(ctx, route, httpRequest))
	if err != nil {
		log.Fatal(err)
	}
	toolbox.Dump(products)
}
