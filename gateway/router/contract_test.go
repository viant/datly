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

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly"
	"github.com/viant/datly/repository"
	rcontract "github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	"github.com/viant/datly/repository/logging"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	xcontract "github.com/viant/xdatly/contract"
	xhandler "github.com/viant/xdatly/handler"
)

type handlerContractRow struct {
	Secret string                       `json:"secret"`
	Mode   int                          `json:"mode" choice:"0:draft" choice:"10:active"`
	Has    *struct{ Secret, Mode bool } `json:"-" setMarker:"true"`
}

func (*handlerContractRow) Contract(_ context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "reader", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[handlerContractRow]() {
			return nil, nil
		}
		switch f.Name {
		case "Secret":
			return f.Hide(), nil
		case "Mode":
			return f.SetTag("enumAccept", "literal").SetTag("enumStyle", "ordinal"), nil
		}
		return nil, nil
	}}, nil
}

type handlerContractInput struct {
	Body *handlerContractRow `parameter:",kind=body"`
}

func (i *handlerContractInput) Init(context.Context) error {
	if i.Body != nil {
		i.Body.Mode++
	}
	return nil
}

type handlerContractOutput struct {
	Body *handlerContractRow `json:"body"`
}
type contractHandler struct{ calls *int }

func (h *contractHandler) Exec(ctx context.Context, session xhandler.Session) (interface{}, error) {
	*h.calls++
	input, ok := ctx.Value(xhandler.InputKey).(*handlerContractInput)
	if !ok {
		return nil, fmt.Errorf("native prepared input is missing")
	}
	if input.Body == nil || input.Body.Secret != "" || input.Body.Mode != 11 || input.Body.Has == nil || !input.Body.Has.Mode || input.Body.Has.Secret {
		return nil, fmt.Errorf("unexpected bound input: %+v", input.Body)
	}
	input.Body.Secret = "server-only"
	return &handlerContractOutput{Body: input.Body}, nil
}
func TestContractNativeHTTPHandler(t *testing.T) {
	for _, engine := range []string{"content.StructologyJSONRuntime", "content.StandardJSONRuntime"} {
		t.Run(engine, func(t *testing.T) { testContractNativeHTTPHandler(t, engine) })
	}
}
func testContractNativeHTTPHandler(t *testing.T, engine string) {
	calls := 0
	target := rcontract.Path{Method: "PATCH", URI: "/contract"}
	c, err := repository.NewComponent(&target, repository.WithHandler(&contractHandler{calls: &calls}), repository.WithInput(reflect.TypeFor[*handlerContractInput]()))
	if err != nil {
		t.Fatal(err)
	}
	c.Content.Marshaller.JSON.Engine = engine
	c.View.Name = "contract"
	c.View.Connector = view.NewConnector("contract", "sqlite3", ":memory:")
	c.View.Schema = state.NewSchema(reflect.TypeFor[struct{}]())
	c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[*handlerContractOutput]())
	if err = c.Init(context.Background(), c.View.GetResource()); err != nil {
		t.Fatal(err)
	}
	registry := repository.NewRegistry("", nil, dispatcher.New)
	registry.Register(c)
	handler := New(&path.Path{Path: target}, nil, registry, nil, "", logging.Config{}, nil)
	request := httptest.NewRequest("PATCH", "http://localhost/contract", strings.NewReader(`{"mode":"active","secret":"blocked","Has":{"Secret":true}}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := handler.handleComponent(context.Background(), request, c)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(response.Body())
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err = json.Unmarshal(raw, &output); err != nil {
		t.Fatalf("%s %v", raw, err)
	}
	body := output["body"].(map[string]any)
	if calls != 1 || body["mode"] != float64(11) || body["secret"] != nil || body["Has"] != nil {
		t.Fatalf("calls=%d output=%s", calls, raw)
	}
}

type scalarEnumInput struct {
	Body int `parameter:",kind=body" choice:"0:draft" choice:"10:active" enumAccept:"literal"`
}
type scalarEnumOutput struct {
	Value string `json:"value" choice:"0:draft" choice:"10:active" enumStyle:"ordinal"`
}
type scalarEnumHandler struct{}

func (*scalarEnumHandler) Exec(ctx context.Context, _ xhandler.Session) (interface{}, error) {
	input, ok := ctx.Value(xhandler.InputKey).(*scalarEnumInput)
	if !ok || input.Body != 10 {
		return nil, fmt.Errorf("unexpected scalar input: %+v", input)
	}
	return &scalarEnumOutput{Value: "active"}, nil
}
func TestContractScalarBodyEnumThroughNativeHTTP(t *testing.T) {
	for _, engine := range []string{"content.StructologyJSONRuntime", "content.StandardJSONRuntime"} {
		t.Run(engine, func(t *testing.T) {
			target := rcontract.Path{Method: "POST", URI: "/scalar"}
			c, err := repository.NewComponent(&target, repository.WithHandler(&scalarEnumHandler{}), repository.WithInput(reflect.TypeFor[*scalarEnumInput]()))
			if err != nil {
				t.Fatal(err)
			}
			c.View.Name = "scalar"
			c.View.Connector = view.NewConnector("scalar", "sqlite3", ":memory:")
			c.View.Schema = state.NewSchema(reflect.TypeFor[struct{}]())
			c.Content.Marshaller.JSON.Engine = engine
			c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[*scalarEnumOutput]())
			if err = c.Init(context.Background(), c.View.GetResource()); err != nil {
				t.Fatal(err)
			}
			registry := repository.NewRegistry("", nil, dispatcher.New)
			registry.Register(c)
			handler := New(&path.Path{Path: target}, nil, registry, nil, "", logging.Config{}, nil)
			request := httptest.NewRequest("POST", "http://localhost/scalar", strings.NewReader(`"active"`))
			request.Header.Set("Content-Type", "application/json")
			response, err := handler.handleComponent(context.Background(), request, c)
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
			if result["value"] != float64(10) {
				t.Fatalf("%s", raw)
			}
		})
	}
}
