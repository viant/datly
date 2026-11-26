package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/repository"
	rcontract "github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	"github.com/viant/datly/repository/logging"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	xhandler "github.com/viant/xdatly/handler"
)

type ordinaryMapInput struct {
	Body map[string]interface{} `parameter:",kind=body"`
}
type ordinaryMapOutput struct {
	Name string `json:"name"`
}
type ordinaryMapHandler struct{}

func (*ordinaryMapHandler) Exec(ctx context.Context, _ xhandler.Session) (interface{}, error) {
	input, ok := ctx.Value(xhandler.InputKey).(*ordinaryMapInput)
	if !ok || input.Body["name"] != "body" {
		return nil, fmt.Errorf("map input not preserved: %+v", input)
	}
	return &ordinaryMapOutput{Name: input.Body["name"].(string)}, nil
}
func TestMapBodyWithoutDynamicContractThroughHTTP(t *testing.T) {
	for _, engine := range []string{"content.StructologyJSONRuntime", "content.StandardJSONRuntime", "content.LegacyJSONRuntime"} {
		t.Run(engine, func(t *testing.T) {
			target := rcontract.Path{Method: "POST", URI: "/map"}
			c, err := repository.NewComponent(&target, repository.WithHandler(&ordinaryMapHandler{}), repository.WithInput(reflect.TypeFor[*ordinaryMapInput]()))
			if err != nil {
				t.Fatal(err)
			}
			c.View.Name = "map"
			c.View.Connector = view.NewConnector("map", "sqlite3", ":memory:")
			c.View.Schema = state.NewSchema(reflect.TypeFor[struct{}]())
			c.Content.Marshaller.JSON.Engine = engine
			c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[*ordinaryMapOutput]())
			if err = c.Init(t.Context(), c.View.GetResource()); err != nil {
				t.Fatal(err)
			}
			dynamic, err := c.HasDynamicContract(target.Method, target.URI)
			if err != nil || dynamic {
				t.Fatalf("unexpected dynamic contract: %t %v", dynamic, err)
			}
			selected, err := c.ResolveContract(t.Context(), nil, target.Method, target.URI)
			if err != nil || selected.Input != nil || selected.Body != nil || selected.Output != nil {
				t.Fatalf("no-hook path selected metadata: %+v %v", selected, err)
			}
			if c.Input.Body.Schema.Type() != reflect.TypeFor[map[string]interface{}]() || c.BodyType() != reflect.TypeFor[struct{}]() {
				t.Fatal("legacy map schema/bootstrap split changed")
			}
			registry := repository.NewRegistry("", nil, dispatcher.New)
			registry.Register(c)
			handler := New(&path.Path{Path: target}, nil, registry, nil, "", logging.Config{}, nil)
			request := httptest.NewRequest("POST", "http://localhost/map?keep=url&name=url", strings.NewReader(`{"name":"body","count":2}`))
			request.Header.Set("Content-Type", "application/json")
			response, err := handler.handleComponent(t.Context(), request, c)
			if engine == "content.LegacyJSONRuntime" {
				// This codec already rejected map destinations before dynamic contracts.
				// Preserve that configured-codec error rather than silently substituting it.
				if err == nil || !strings.Contains(err.Error(), "unsupported unmarshall to map type") {
					t.Fatalf("legacy codec behavior changed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(response.Body())
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			if err = json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			if result["name"] != "body" || request.URL.Query().Get("name") != "body" || request.URL.Query().Get("keep") != "url" || request.URL.Query().Get("count") != "2" {
				t.Fatalf("body=%s query=%v", raw, request.URL.Query())
			}
		})
	}
}
