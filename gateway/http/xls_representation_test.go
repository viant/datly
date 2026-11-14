package http

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/x/shape"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type representedXLSKey struct{}
type representedXLSBody struct {
	Code    int `json:"code" mcpEnum:"true" choice:"1:ONE" enumAccept:"literal"`
	private any
}

func (b *representedXLSBody) UnmarshalXLS(ctx context.Context, raw []byte) error {
	b.Code = len(raw)
	b.private = ctx.Value(representedXLSKey{})
	return nil
}
func TestHTTPRequestScopeRepresentedXLS(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, hidden := range []bool{false, true} {
			rewrite, err := shape.CompileRewrite(reflect.TypeFor[*representedXLSBody]())
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
			marker := new(int)
			req := httptest.NewRequest("PATCH", "/xls", strings.NewReader("book")).WithContext(context.WithValue(t.Context(), representedXLSKey{}, marker))
			req.Header.Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=utf-8")
			scope, err := newHTTPRequestScope(req, nil, deferred)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			bound, err := injector.ForScope(scope.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var actual struct{ Body *representedXLSBody }
			plan, err := injector.CompilePlan(reflect.TypeOf(actual), bindly.BindingSpec{Path: "Body", Location: state.Location{Kind: "body"}, BodyRepresentation: metadata})
			if err != nil {
				t.Fatal(err)
			}
			err = bound.Bind(req.Context(), &actual, bindly.WithPlan(plan))
			if hidden {
				if err == nil || actual.Body != nil {
					t.Fatalf("hidden policy accepted: %v", err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if actual.Body == nil || actual.Body.Code != 4 || actual.Body.private != marker {
				t.Fatal("canonical workbook provenance lost")
			}
		}
	}
}
