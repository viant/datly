package handlerreport

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type LinkedComponents struct {
	Linked xdatly.Component[Input, Output] `component:"LinkedReport,path=/linked-report,method=GET,connector=main,handler=NewReport,report=true" mcp:"[{\"kind\":\"tool\",\"name\":\"LinkedReport\"}]"`
}

func (LinkedComponents) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewReport" {
		return custom.Factory(NewReport)
	}
	return nil
}

type RegistryComponents struct {
	Legacy xdatly.Component[Input, Output] `component:"RegistryReport,path=/registry-report,method=GET,connector=main,handler=NewReport,report=true" mcp:"[{\"kind\":\"tool\",\"name\":\"RegistryReport\"}]"`
	Read   xdatly.Component[Input, Output] `component:"PrivateReport,path=/private-report,method=GET,connector=main,internal=true"`
}

var LinkedDatly = new(LinkedComponents)
var RegistryDatly = new(RegistryComponents)
var LinkedDatlyType = reflect.TypeFor[LinkedComponents]()
var RegistryDatlyType = reflect.TypeFor[RegistryComponents]()

type Input struct {
	Permit bool `parameter:"Permit,kind=query,in=permit" predicate:"equal,f,enabled" json:"permit"`
}

type Output struct {
	Data    []*Row `parameter:",kind=output,in=view" view:"facts,groupable=true,selectorProjection=true" sql:"SELECT f.country, f.region, f.site_id, SUM(f.amount) AS amount FROM report_facts f WHERE f.enabled=:Permit GROUP BY f.country, f.region, f.site_id" json:"data"`
	Handled bool   `json:"handled"`
}

type Row struct {
	Country       string  `sqlx:"country" json:"country" groupable:"true"`
	Region        string  `sqlx:"region" json:"region" groupable:"true"`
	SiteID        int     `sqlx:"site_id" json:"siteId" groupable:"true"`
	Amount        int     `sqlx:"amount" json:"amount"`
	CountryRegion *Region `view:"countryRegion" on:"Country:f.country=Country:country,Region:f.region=Region:region" sql:"SELECT country, region, label FROM report_regions" json:"countryRegion"`
	Site          *Site   `view:"site" on:"SiteID:f.site_id=ID:id" sql:"SELECT id, label FROM report_sites" json:"site"`
}

type Region struct {
	Country string `sqlx:"country" json:"country"`
	Region  string `sqlx:"region" json:"region"`
	Label   string `sqlx:"label" json:"label"`
}

type Site struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
}

type reportHandler struct{}

func NewReport() handler.Contract[Input, Output] { return &reportHandler{} }

func (*reportHandler) Exec(ctx context.Context, session handler.Session, input *Input, output *Output) error {
	if !input.Permit {
		return &response.Error{Code: 403, Payload: "handler authorization required"}
	}
	value, found, err := session.Binder().Lookup(ctx, exec.ComponentInvokerKey)
	if err != nil {
		return err
	}
	invoker, ok := value.(exec.ComponentInvoker)
	if !found || !ok {
		return fmt.Errorf("component invoker unavailable")
	}
	selectors, found, err := session.Binder().Lookup(ctx, handler.SelectorsKey)
	if err != nil {
		return err
	}
	var providers []locator.Provider
	if found {
		providers = append(providers, provider.Static(handler.SelectorsKey, selectors))
	}
	childContext := exec.CaptureChildOutputSelection(ctx)
	value, err = invoker.InvokeComponent(childContext, exec.ComponentRequest{
		Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[RegistryComponents]().PkgPath(), Name: "PrivateReport"}, Route: spec.RouteRef{Method: "GET", Path: "/private-report"}},
		Input:  input, Providers: providers,
	})
	if err != nil {
		return err
	}
	child, ok := value.(*Output)
	if !ok {
		return fmt.Errorf("unexpected reader result %T", value)
	}
	*output = *child
	output.Handled = true
	exec.PublishOutputSelection(ctx, output, exec.SelectedOutputFields(childContext, child))
	return nil
}

func Exports() (*x.Registry, error) {
	r := x.NewRegistry()
	factory, err := x.NewFunction(reflect.TypeFor[RegistryComponents]().PkgPath(), "NewReport", custom.Factory(NewReport))
	if err != nil {
		return nil, err
	}
	return r, r.RegisterFunctions(factory)
}
