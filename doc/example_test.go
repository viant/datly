package doc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
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

// Example shows how to programmatically execute executor rule.
func Example() {
	//Uncomment various additional debugging and troubleshuting
	// expand.SetPanicOnError(false)
	// read.ShowSQL(true)
	// update.ShowSQL(true)
	// insert.ShowSQL(true)

	ctx := context.Background()
	service, _ := datly.New(context.Background())
	ruleURL := "yyyyyyy/Datly/routes/dev/product.yaml"

	components, err := service.LoadComponents(ctx, ruleURL, repository.WithPackageTypes(
		view.NewPackagedType("domain", "Product", reflect.TypeOf(Product{})),
		view.NewPackagedType("domain", "Validation", reflect.TypeOf(Validation{}))),
	)
	if err != nil {
		log.Fatal(err)
	}
	httpRequest, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:8080/v1/api/dev", io.NopCloser(strings.NewReader(`{"Name":"IPad"}`)))
	if err != nil {
		log.Fatal(err)
	}
	err = service.SignRequest(httpRequest, &jwt.Claims{
		Email:  "dev@viantinc.com",
		UserID: 111,
	})
	if err != nil {
		log.Fatal(err)
	}
	aComponent := components.Components[0]
	aSession := service.NewComponentSession(aComponent, datly.WithRequest(httpRequest))
	response, err := service.Operate(ctx, datly.WithComponent(aComponent), datly.WithSession(aSession))
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(response)
	fmt.Printf("%T, %s\n", response, data)
}
