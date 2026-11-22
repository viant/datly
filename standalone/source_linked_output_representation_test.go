package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/app/outputrepresentation"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x/shape"
)

func TestStandaloneLinkedOutputRepresentation(t *testing.T) {
	const wireName = "exact_WireName"
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			ctx := t.Context()
			options, target := linkedOutputRepresentationOptions(t, eager, wireName)
			server, err := New(ctx, options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			delete(options.LinkedOutputRepresentations, target)
			require.NoError(t, server.Reload(ctx, 1))

			// Check OpenAPI before the first business request to exercise lazy discovery.
			document := httptest.NewRecorder()
			server.ServeHTTP(document, httptest.NewRequest(http.MethodGet, gateway.DefaultOpenAPIURI+target.Route.Path, nil))
			require.Equal(t, http.StatusOK, document.Code, document.Body.String())
			require.Contains(t, document.Body.String(), `"`+wireName+`"`)
			require.NotContains(t, document.Body.String(), `"originalValue"`)

			for path, field := range map[string]string{
				"/represented": wireName, "/represented-sibling": wireName,
				"/canonical": "originalValue", "/canonical-sibling": "originalValue",
			} {
				response := httptest.NewRecorder()
				server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.JSONEq(t, fmt.Sprintf(`{"%s":37}`, field), response.Body.String())
			}
			result, err := server.manager.InvokeComponent(ctx, exec.ComponentRequest{
				Target: target, Input: &outputrepresentation.Input{},
			})
			require.NoError(t, err)
			require.Equal(t, &outputrepresentation.Output{OriginalValue: 37}, result)

			native := (mcpclient.Config{Source: server.manager}).New(t)
			listing, err := native.ListTools(ctx, nil)
			require.NoError(t, err)
			expected := []string{"default", "u", "l", "d", "ld", "ud", "lc", "uc", "lu", "uu", "t", "s"}
			var found []string
			for _, tool := range listing.Tools {
				require.True(t, strings.HasPrefix(tool.Name, "LinkedOutput_"), tool.Name)
				found = append(found, strings.TrimPrefix(tool.Name, "LinkedOutput_"))
				require.NotNil(t, tool.OutputSchema)
				require.Len(t, tool.OutputSchema.Properties, 1, tool.Name)
				require.Contains(t, tool.OutputSchema.Properties, wireName, tool.Name)
				result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: tool.Name, Arguments: map[string]any{}})
				require.NoError(t, err, tool.Name)
				require.False(t, result.IsError != nil && *result.IsError, "%s: %+v", tool.Name, result)
				payload, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				require.JSONEq(t, fmt.Sprintf(`{"%s":37}`, wireName), string(payload), tool.Name)
			}
			require.ElementsMatch(t, expected, found)
		})
	}
}

func TestStandaloneRejectsInvalidLinkedOutputRepresentation(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			for _, tc := range []struct {
				name, want string
				change     func(*testing.T, *Options, exec.ComponentTarget)
			}{
				{"wrong route", "target is not selected", func(t *testing.T, options *Options, target exec.ComponentTarget) {
					wrong := target
					wrong.Route.Path = "/canonical"
					options.LinkedOutputRepresentations[wrong] = options.LinkedOutputRepresentations[target]
					delete(options.LinkedOutputRepresentations, target)
				}},
				{"wrong kind", "target is not selected", func(t *testing.T, options *Options, target exec.ComponentTarget) {
					wrong := target
					wrong.Component.Kind = spec.KindView
					options.LinkedOutputRepresentations[wrong] = options.LinkedOutputRepresentations[target]
					delete(options.LinkedOutputRepresentations, target)
				}},
				{"wrong linked type", "does not match its linked type", func(t *testing.T, options *Options, target exec.ComponentTarget) {
					walker, err := shape.CompileRewrite(reflect.TypeFor[outputrepresentation.Input]())
					require.NoError(t, err)
					options.LinkedOutputRepresentations[target], err = walker.RewriteRepresentation(nil)
					require.NoError(t, err)
				}},
				{"multiple declarations", "multiple linked output representations", func(t *testing.T, options *Options, target exec.ComponentTarget) {
					sibling := target
					sibling.Route.Path = "/represented-sibling"
					options.LinkedOutputRepresentations[sibling] = options.LinkedOutputRepresentations[target]
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					options, target := linkedOutputRepresentationOptions(t, eager, "exact_WireName")
					tc.change(t, &options, target)
					ctx := t.Context()
					server, err := New(ctx, options)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
					require.ErrorContains(t, server.Reload(ctx, 1), tc.want)
					require.Zero(t, server.manager.Revision())
				})
			}
		})
	}
}

func linkedOutputRepresentationOptions(t *testing.T, eager bool, wireName string) (Options, exec.ComponentTarget) {
	t.Helper()
	scope := reflect.TypeFor[outputrepresentation.LinkedComponents]().PkgPath()
	walker, err := shape.CompileRewrite(reflect.TypeFor[outputrepresentation.Output]())
	require.NoError(t, err)
	representation, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		return &shape.FieldMetadata{Name: field.Name, Tag: reflect.StructTag(fmt.Sprintf(`json:"%s"`, wireName))}, nil
	})
	require.NoError(t, err)
	target := exec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Represented", Scope: scope},
		Route:     spec.RouteRef{Method: http.MethodGet, Path: "/represented"},
	}
	return Options{
		Config: &config.Config{
			Endpoint:    config.Endpoint{Address: "127.0.0.1:0"},
			GoBootstrap: &config.Packages{Packages: []string{scope}, LinkedOnly: !eager, EagerComponents: eager},
			Config:      gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Linked output representation", Version: "1"}}},
		},
		LinkedOutputRepresentations: map[exec.ComponentTarget]*shape.Representation{target: representation},
	}, target
}
