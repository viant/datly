package openapi_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func (a documentAssertion) references(value any) {
	a.t.Helper()
	switch node := value.(type) {
	case map[string]any:
		if ref, ok := node["$ref"].(string); ok {
			require.Len(a.t, node, 1, "OpenAPI Reference Objects cannot carry schema siblings")
			require.True(a.t, strings.HasPrefix(ref, "#/components/schemas/"))
			require.Contains(a.t, a.document.Components.Schemas, strings.TrimPrefix(ref, "#/components/schemas/"))
		}
		for _, value := range node {
			a.references(value)
		}
	case []any:
		for _, value := range node {
			a.references(value)
		}
	}
}

func TestDocumentReferencesIsolationAndMethods(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Methods"}}
	methods := []string{"GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH", "TRACE"}
	for _, method := range methods {
		component.Routes = append(component.Routes, &spec.Route{Method: method, Path: "/records"})
	}
	entry := (fixture{component: component}).registration(t)
	request := documentRequest(entry)
	doc, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(data, &wire))
	(documentAssertion{t, doc}).references(wire)
	path := wire["paths"].(map[string]any)["/records"].(map[string]any)
	require.Len(t, path, len(methods))
	ids := map[string]bool{}
	for _, method := range methods {
		operation := path[strings.ToLower(method)].(map[string]any)
		id := operation["operationId"].(string)
		require.False(t, ids[id])
		ids[id] = true
		require.Contains(t, operation["responses"], "200")
	}
	require.Empty(t, doc.Paths["/records"].Head.Responses["200"].Content)
	for _, schema := range doc.Components.Schemas {
		schema.Properties = nil
	}
	again, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	againData, err := json.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, string(data), string(againData))
}

func TestSchemaIdentityAndCrossMethodPathCollisions(t *testing.T) {
	left := func() reflect.Type { type Item struct{ Left string }; return reflect.TypeFor[Item]() }()
	right := func() reflect.Type { type Item struct{ Right int }; return reflect.TypeFor[Item]() }()
	a := (fixture{output: left}).registration(t)
	b := (fixture{output: right, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Other"}, Routes: []*spec.Route{{Method: "GET", Path: "/other"}}}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(a, b))
	require.NoError(t, err)
	first := doc.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema
	second := doc.Paths["/other"].Get.Responses["200"].Content["application/json"].Schema
	require.NotEqual(t, first.Ref, second.Ref)
	require.Contains(t, (documentAssertion{t, doc}).deref(first).Properties, "Left")
	require.Contains(t, (documentAssertion{t, doc}).deref(second).Properties, "Right")
	type inputA struct {
		ID int `parameter:"ID,kind=path,in=id"`
	}
	type inputB struct {
		ID int `parameter:"ID,kind=path,in=other"`
	}
	a = (fixture{input: reflect.TypeFor[inputA](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "A"}, Routes: []*spec.Route{{Method: "GET", Path: "/records/{id}"}}}}).registration(t)
	b = (fixture{input: reflect.TypeFor[inputB](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "B"}, Routes: []*spec.Route{{Method: "POST", Path: "/records/{other}"}}}}).registration(t)
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(a, b))
	require.ErrorContains(t, err, "equivalent OpenAPI paths")
}

func TestJSONNameAndMethodRepresentability(t *testing.T) {
	type dashOutput struct {
		Value string `json:"-,omitempty"`
	}
	entry := (fixture{output: reflect.TypeFor[dashOutput]()}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	a := documentAssertion{t, doc}
	schema := a.deref(doc.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema)
	require.Contains(t, schema.Properties, "-")
	wire, err := json.Marshal(dashOutput{Value: "value"})
	require.NoError(t, err)
	require.JSONEq(t, `{"-":"value"}`, string(wire))
	bad, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Value", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"bad\\name"`)}})
	require.NoError(t, err)
	entry = (fixture{output: bad}).registration(t)
	doc, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	require.Contains(t, (documentAssertion{t, doc}).deref(doc.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema).Properties, "Value")
	type bodyInput struct {
		Data string `parameter:"Data,kind=body"`
	}
	entry = (fixture{input: reflect.TypeFor[bodyInput](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Body"}, Routes: []*spec.Route{{Method: "GET", Path: "/body"}}}}).registration(t)
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.ErrorContains(t, err, "request body on GET")
}

func TestAuthorizationRequiresActualAPIKeyPolicy(t *testing.T) {
	type input struct {
		Token string `parameter:"Token,kind=header,in=Authorization"`
	}
	entry := (fixture{input: reflect.TypeFor[input](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Authorized"}, Routes: []*spec.Route{{Method: "POST", Path: "/records", APIKeyHeader: "Authorization", APIKeyValue: "opaque-value"}}}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	operation := doc.Paths["/records"].Post
	require.Empty(t, operation.Parameters)
	require.Len(t, *operation.Security, 1)
	for _, scheme := range doc.Components.SecuritySchemes {
		require.Equal(t, "apiKey", scheme.Type)
		require.Equal(t, "Authorization", scheme.Name)
		require.Empty(t, scheme.Scheme)
	}
}
