package doc

import (
	"context"
	"fmt"
	"github.com/viant/datly"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"io"
	"log"
	"net/http"
	surl "net/url"
	"reflect"
	"strings"
	"time"
)

type Product struct {
	Id       int
	Name     string
	VendorId int
}

type Validation struct {
	IsValid bool
}

//Example_RuleExecution show how to programmatically execute rule
func Example_RuleExecution() {

	//Uncomment various additional debugging and troubleshuting
	// expand.SetPanicOnError(false)
	// read.ShowSQL(true)
	// update.ShowSQL(true)
	// insert.ShowSQL(true)

	ctx := context.Background()

	service := datly.New(datly.NewConfig())
	viewName := "product"
	ruleURL := fmt.Sprintf("yyyyyyy/Datly/routes/dev/%v.yaml", viewName)
	err := service.LoadRoute(ctx, ruleURL,
		view.NewPackagedType("domain", "Product", reflect.TypeOf(Product{})),
		view.NewPackagedType("domain", "Validation", reflect.TypeOf(Validation{})),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = service.Init(ctx)
	if err != nil {
		log.Fatal(err)
	}

	//Create http Patch request for example
	URL, _ := surl.Parse("http://127.0.0.1:8080/v1/api/dev")
	httpRequest := &http.Request{
		URL:    URL,
		Method: "PUT",
		Body:   io.NopCloser(strings.NewReader(`{"Entity":{"VendorId":5672}}`)),
		Header: http.Header{},
	}
	token, err := service.JwtSigner.Create(time.Hour, &jwt.Claims{
		Email:  "dev@viantinc.com",
		UserID: 111,
	})
	if err != nil {
		log.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+token)
	routeRes, _ := service.Routes()
	route := routeRes.Routes[0] //make sure you are using correct route
	err = service.Exec(ctx, viewName, datly.WithExecHttpRequest(ctx, route, httpRequest))
	if err != nil {
		log.Fatal(err)
	}
}
