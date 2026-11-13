package tool

import (
	"context"
	"encoding/base64"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x/shape"
	"reflect"
	"testing"
)

type representedBinaryKey struct{}
type representedBinaryBody struct {
	Code    int `json:"code" mcpEnum:"true" choice:"1:ONE" enumAccept:"literal"`
	private any
}

func (b *representedBinaryBody) UnmarshalXLS(ctx context.Context, raw []byte) error {
	b.Code = len(raw)
	b.private = ctx.Value(representedBinaryKey{})
	return nil
}
func TestBinaryHandlerRepresentedAnonymousXLS(t *testing.T) {
	type input struct {
		Body *representedBinaryBody `mcp:"name=Body"`
	}
	for _, hidden := range []bool{false, true} {
		rewrite, err := shape.CompileRewrite(reflect.TypeFor[*representedBinaryBody]())
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := rewrite.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
			if f.Name != "Code" {
				return nil, nil
			}
			f.Tag = `json:"code" mcpEnum:"true"`
			if hidden {
				f.Tag = `json:"-"`
			}
			return f, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		binding := bindly.BindingSpec{Path: "Body", Location: state.Location{Kind: "body"}, BodyRepresentation: metadata}
		route := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
		plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/files", Name: "represented"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "files.represented"}, Contract: route})
		if err != nil {
			t.Fatal(err)
		}
		marker := new(int)
		called := false
		component := binaryComponentFunc(func(ctx context.Context, r exec.ComponentRequest) (interface{}, error) {
			called = true
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			bound, err := injector.ForScope(r.Providers...)
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := injector.CompilePlan(reflect.TypeFor[input](), binding)
			if err != nil {
				t.Fatal(err)
			}
			var actual input
			err = bound.Bind(ctx, &actual, bindly.WithPlan(compiled))
			if hidden {
				if err == nil || actual.Body != nil {
					t.Fatalf("hidden policy accepted: %v", err)
				}
				return nil, err
			}
			if err != nil {
				t.Fatal(err)
			}
			if actual.Body == nil || actual.Body.Code != 4 || actual.Body.private != marker {
				t.Fatal("canonical binary provenance lost")
			}
			return map[string]interface{}{"ok": true}, nil
		})
		request := &schema.CallToolRequest{}
		request.Params.Name = plan.Metadata().Name
		request.Params.Arguments = map[string]interface{}{"blob": map[string]interface{}{"data": base64.StdEncoding.EncodeToString([]byte("book"))}}
		_, protocolErr := NewHandler(plan, invocation.New(invocation.Config{Invoker: component})).Handle(context.WithValue(t.Context(), representedBinaryKey{}, marker), request)
		if protocolErr != nil {
			t.Fatal(protocolErr)
		}
		if !called {
			t.Fatal("binary invocation did not reach represented binding")
		}
	}
}
