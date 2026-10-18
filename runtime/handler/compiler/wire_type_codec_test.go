package compiler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/mcp/tool"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type codecWireRow struct {
	ID int `json:"id"`
}
type codecWireRows []*codecWireRow
type codecWireInput struct {
	Data  []*codecWireRow `parameter:"Data,kind=body,in=data,dataType=json.RawMessage,required=false,errorCode=400" codec:"array"`
	Other json.RawMessage `parameter:"Other,kind=body,in=other,required=false"`
}
type codecWireFactory struct{ calls int }

func (f *codecWireFactory) New(c *codec.Config, _ ...codec.Option) (codec.Instance, error) {
	if c.SourceType != reflect.TypeFor[json.RawMessage]() || c.DestinationType != reflect.TypeFor[[]*codecWireRow]() {
		return nil, fmt.Errorf("codec representation changed")
	}
	return f, nil
}
func (f *codecWireFactory) Value(_ context.Context, raw any, _ ...codec.Option) (any, error) {
	f.calls++
	if raw == nil {
		return []*codecWireRow(nil), nil
	}
	data := bytes.TrimSpace(raw.(json.RawMessage))
	if len(data) > 0 && data[0] != '[' && !bytes.Equal(data, []byte("null")) {
		return nil, &response.Error{Code: 400, Payload: map[string]any{"status": "error", "message": "failed to unmarshal data, expected array"}}
	}
	var rows []*codecWireRow
	err := json.Unmarshal(data, &rows)
	return rows, err
}
func TestExplicitWireTypePreservesRawMessageCodecBinding(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			optional := false
			parameter := &spec.Parameter{Name: "Data", TypeExpr: "json.RawMessage", OutputTypeExpr: "[]*codecWireRow", Source: spec.BindSource{Kind: "body", Name: "data"}, Required: &optional, ErrorStatusCode: 400, Codec: &spec.Codec{Body: "array"}}
			if explicit {
				parameter.WireTypeExpr = "codecWireRows"
			}
			component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/codec-wire"}}, Parameters: []*spec.Parameter{parameter, {Name: "Other", TypeExpr: "json.RawMessage", Source: spec.BindSource{Kind: "body", Name: "other"}, Required: &optional}}}
			factory := &codecWireFactory{}
			compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[codecWireInput](), CodecFactory: factory, TypeLookup: func(name string) (reflect.Type, error) {
				if name == "codecWireRows" {
					return reflect.TypeFor[codecWireRows](), nil
				}
				if strings.Contains(name, "RawMessage") {
					return reflect.TypeFor[json.RawMessage](), nil
				}
				return nil, nil
			}}).Compile()
			require.NoError(t, err)
			route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/codec-wire"})
			require.True(t, ok)
			field := route.Fields()[0]
			require.Equal(t, reflect.TypeFor[json.RawMessage](), field.SourceType())
			require.Equal(t, reflect.TypeFor[[]*codecWireRow](), field.DestinationType())
			require.NotNil(t, field.Binding().Transformer)
			require.False(t, *field.Binding().Required)
			require.Equal(t, 400, field.Binding().ErrorCode)
			plan, err := tool.NewCompiler().Compile(tool.Input{Component: spec.Key{Kind: spec.KindComponent, Name: "wire"}, Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "wire"}})
			require.NoError(t, err)
			require.Zero(t, factory.calls)
			props := plan.Metadata().InputSchema.Properties
			require.Empty(t, props["Other"])
			if explicit {
				require.Contains(t, props["Data"], "items")
			} else {
				require.Empty(t, props["Data"])
			}
			for _, raw := range []string{`{"data":[{"id":7}]}`, `{"data":[]}`, `{"data":null}`, `{}`, `{"data":{}}`} {
				t.Run(raw, func(t *testing.T) {
					var object map[string]any
					require.NoError(t, json.Unmarshal([]byte(raw), &object))
					args := map[string]interface{}{}
					if value, ok := object["data"]; ok {
						args["Data"] = value
					}
					scope, err := plan.Scope(args)
					require.NoError(t, err)
					defer scope.Close()
					injector, err := bindly.NewInjector()
					require.NoError(t, err)
					scoped, err := injector.ForScope(scope.Providers()...)
					require.NoError(t, err)
					var actual codecWireInput
					before := factory.calls
					err = scoped.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
					present := len(object) > 0
					if strings.Contains(raw, `"data":{}`) {
						require.Error(t, err)
						var body response.BodyError
						require.ErrorAs(t, err, &body)
						require.Equal(t, map[string]any{"status": "error", "message": "failed to unmarshal data, expected array"}, body.ResponseBody())
					} else {
						require.NoError(t, err)
					}
					if present {
						require.Equal(t, before+1, factory.calls)
					} else {
						require.Equal(t, before, factory.calls)
					}
					actualErr := err
					req := httptest.NewRequest("POST", "/codec-wire", strings.NewReader(raw))
					req.Header.Set("Content-Type", "application/json")
					httpScope, err := requestprovider.New(req)
					require.NoError(t, err)
					defer httpScope.Close()
					httpInjector, err := injector.ForScope(httpScope.Providers()...)
					require.NoError(t, err)
					var expected codecWireInput
					httpErr := httpInjector.Bind(t.Context(), &expected, bindly.WithPlan(route.Plan()))
					require.Equal(t, actualErr == nil, httpErr == nil)
					require.Equal(t, expected.Data, actual.Data)
				})
			}
		})
	}
}
