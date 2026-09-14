package openapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type child struct {
	Value string `json:"value"`
	Next  *child `json:"next,omitempty"`
}
type patch struct {
	Name     string                               `json:"name"`
	Note     *string                              `json:"note"`
	Children []*child                             `json:"children"`
	Has      *struct{ Name, Note, Children bool } `setMarker:"true"`
	Secret   string                               `json:"-"`
}
type envelope struct {
	Data     []child              `json:"data"`
	Detail   *child               `json:"detail"`
	Tags     map[string]*string   `json:"tags,omitempty"`
	At       time.Time            `json:"at"`
	Bytes    []byte               `json:"bytes"`
	Fixed    [2]int               `json:"fixed"`
	Internal string               `json:"-"`
	Has      *struct{ Data bool } `json:"-" setMarker:"true"`
}
type transportInput struct {
	Search   string `parameter:"Search,kind=query,in=search,required=true"`
	ID       *int   `parameter:"ID,kind=path,in=id,uri=/{id},required=true"`
	IDs      []int  `parameter:"IDs,kind=query,in=ids" json:"differentIDs"`
	Tenant   string `parameter:"Tenant,kind=header,in=X-Tenant,required=true"`
	Session  string `parameter:"Session,kind=cookie,in=session"`
	Payload  *patch `parameter:"Payload,kind=body,required=true"`
	Internal string
}

type fixture struct {
	component     *spec.Component
	input, output reflect.Type
	execute       handler.HandlerFunc
}

func (f fixture) registration(t *testing.T) *registry.RegisteredComponent {
	t.Helper()
	entry, err := f.build()
	require.NoError(t, err)
	return entry
}

func (f fixture) build() (*registry.RegisteredComponent, error) {
	if f.component == nil {
		f.component = &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records", Scope: "example.com/public"}, Routes: []*spec.Route{{Method: "POST", Path: "/records"}}}
	}
	if f.input == nil {
		f.input = reflect.TypeFor[struct{}]()
	}
	if f.output == nil {
		f.output = reflect.TypeFor[envelope]()
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: f.input, OutputType: f.output})
	if err != nil {
		return nil, err
	}
	if f.execute == nil {
		f.execute = func(context.Context, handler.Invocation) (any, error) { return &envelope{}, nil }
	}
	return artifact.Registration(registry.RegisteredComponent{Handler: f.execute})
}
func documentRequest(entries ...*registry.RegisteredComponent) openapi.Request {
	return openapi.Request{Info: openapi3.Info{Title: "Records", Version: "1.0.0"}, Components: entries}
}

type documentAssertion struct {
	t        *testing.T
	document *openapi3.OpenAPI
}

func (a documentAssertion) deref(s *openapi3.Schema) *openapi3.Schema {
	a.t.Helper()
	if s.Ref == "" {
		return s
	}
	require.True(a.t, strings.HasPrefix(s.Ref, "#/components/schemas/"))
	result := a.document.Components.Schemas[strings.TrimPrefix(s.Ref, "#/components/schemas/")]
	require.NotNil(a.t, result, "unresolved %s", s.Ref)
	return result
}
func (a documentAssertion) parameter(op *openapi3.Operation, kind, name string) *openapi3.Parameter {
	a.t.Helper()
	for _, p := range op.Parameters {
		if p.In == kind && p.Name == name {
			return p
		}
	}
	a.t.Fatalf("parameter %s %s not found", kind, name)
	return nil
}

func TestBootstrapTransportDocumentAndHTTP(t *testing.T) {
	entry := (fixture{input: reflect.TypeFor[transportInput](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*transportInput)
		require.Equal(t, "tenant-a", input.Tenant)
		require.Equal(t, "term", input.Search)
		require.Equal(t, []int{3, 7}, input.IDs)
		require.Equal(t, "cookie-a", input.Session)
		require.NotNil(t, input.Payload)
		require.True(t, input.Payload.Has.Name)
		require.False(t, input.Payload.Has.Note, "forged presence must be ignored")
		require.Equal(t, "Example", input.Payload.Name)
		if input.ID != nil {
			require.Equal(t, 9, *input.ID)
		}
		return &envelope{Data: []child{{Value: input.Payload.Name}}}, nil
	}}).registration(t)
	require.Len(t, entry.Component.Routes, 2, "bootstrap expands WithURI")
	request := documentRequest(entry)
	doc, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "3.0.1", doc.OpenAPI)
	require.Len(t, doc.Paths, 2)
	a := documentAssertion{t, doc}
	base := doc.Paths["/records"].Post
	alternate := doc.Paths["/records/{id}"].Post
	require.Len(t, base.Parameters, 4)
	require.True(t, a.parameter(base, "query", "search").Required)
	require.Equal(t, "string", a.parameter(base, "query", "search").Schema.Type)
	id := a.parameter(alternate, "path", "id")
	require.True(t, id.Required)
	require.Equal(t, "integer", id.Schema.Type)
	require.False(t, id.Schema.Nullable)
	ids := a.parameter(base, "query", "ids")
	require.Equal(t, "array", ids.Schema.Type)
	require.Equal(t, "integer", ids.Schema.Items.Type)
	require.True(t, *ids.Explode)
	require.False(t, ids.Required)
	require.True(t, a.parameter(base, "header", "X-Tenant").Required)
	require.False(t, a.parameter(base, "cookie", "session").Required)
	require.True(t, base.RequestBody.Required)
	body := a.deref(base.RequestBody.Content["application/json"].Schema)
	require.Contains(t, body.Properties, "name")
	require.NotContains(t, body.Properties, "Has")
	require.NotContains(t, body.Properties, "Secret")
	require.True(t, body.Properties["note"].Nullable)
	require.Equal(t, "array", body.Properties["children"].Type)
	require.Len(t, body.Properties["children"].Items.AnyOf, 2)
	require.NotEmpty(t, body.Properties["children"].Items.AnyOf[0].Ref)
	response := a.deref(base.Responses["200"].Content["application/json"].Schema)
	require.Equal(t, "array", response.Properties["data"].Type)
	require.NotEmpty(t, response.Properties["data"].Items.Ref)
	require.Len(t, response.Properties["detail"].AnyOf, 2)
	require.NotContains(t, response.Properties, "Internal")
	require.NotContains(t, response.Properties, "Has")
	require.Contains(t, response.Required, "detail")
	require.NotContains(t, response.Required, "tags")
	require.NotNil(t, response.Properties["tags"].AdditionalProperties)
	require.Equal(t, "date-time", response.Properties["at"].Format)
	require.Equal(t, "byte", response.Properties["bytes"].Format)
	require.EqualValues(t, 2, *response.Properties["fixed"].MaxItems)
	require.Len(t, base.Responses, 1)
	require.Empty(t, doc.Components.SecuritySchemes)
	require.Nil(t, base.Security)
	encoded, err := json.Marshal(doc)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"additionalProperties"`)
	var roundTrip openapi3.OpenAPI
	require.NoError(t, json.Unmarshal(encoded, &roundTrip))
	require.Equal(t, doc.Paths, roundTrip.Paths)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, path := range []string{"/records", "/records/9"} {
		req := httptest.NewRequest("POST", path+"?ids=3&ids=7&search=term", strings.NewReader(`{"name":"Example","Has":{"Note":true}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Tenant", "tenant-a")
		req.AddCookie(&http.Cookie{Name: "session", Value: "cookie-a"})
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
		require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
		var actual map[string]any
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &actual))
		require.Contains(t, actual, "data")
		require.Contains(t, actual, "detail")
		require.NotContains(t, actual, "Internal")
		require.NotContains(t, actual, "Has")
	}
	for _, missing := range []string{"query", "header", "body"} {
		t.Run("required "+missing, func(t *testing.T) {
			query := "?ids=3&ids=7&search=term"
			payload := `{"name":"Example"}`
			if missing == "query" {
				query = "?ids=3&ids=7"
			}
			if missing == "body" {
				payload = ""
			}
			req := httptest.NewRequest("POST", "/records"+query, strings.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			if missing != "header" {
				req.Header.Set("X-Tenant", "tenant-a")
			}
			recorder := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
			require.GreaterOrEqual(t, recorder.Code, 400, recorder.Body.String())
		})
	}

}

func TestSelectedBodyNamesAndRequiredness(t *testing.T) {
	type input struct {
		Name  string  `parameter:"Name,kind=body,in=display_name,required=true" json:"other"`
		Items []child `parameter:"Items,kind=body,in=items"`
	}
	entry := (fixture{input: reflect.TypeFor[input](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		require.Equal(t, "Example", inv.Input.(*input).Name)
		require.Len(t, inv.Input.(*input).Items, 1)
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	body := doc.Paths["/records"].Post.RequestBody
	require.True(t, body.Required)
	require.Equal(t, []string{"display_name"}, body.Content["application/json"].Schema.Required)
	require.Contains(t, body.Content["application/json"].Schema.Properties, "display_name")
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/records", strings.NewReader(`{"display_name":"Example","items":[{"value":"child"}]}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
}

func TestDocumentSnapshotAndRuntimeExposure(t *testing.T) {
	public := (fixture{}).registration(t)
	private := (fixture{component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Private", Scope: "example.com/private"}, Routes: []*spec.Route{{Method: "GET", Path: "/private"}}}}).registration(t)
	entries := []*registry.RegisteredComponent{public, private}
	rt, err := druntime.NewRuntime(entries, druntime.WithExposedPackages([]string{"example.com/public"}, nil))
	require.NoError(t, err)
	request := documentRequest(entries...)
	request.Visibility = rt
	doc, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, doc.Paths, 1)
	require.NotContains(t, doc.Paths, "/private")
	served, err := openapi.NewHandler(context.Background(), request)
	require.NoError(t, err)
	public.Component.Description = "later mutation"
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := (openapi.Generator{}).Generate(context.Background(), request)
			require.NoError(t, err)
			require.NotNil(t, result)
		}()
	}
	wg.Wait()
	mux := http.NewServeMux()
	mux.Handle("/openapi.json", served)
	for _, tc := range []struct {
		method string
		status int
		body   bool
	}{{"GET", 200, true}, {"HEAD", 200, false}, {"POST", 405, false}} {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(tc.method, "/openapi.json", nil))
		require.Equal(t, tc.status, recorder.Code)
		require.Equal(t, tc.body, recorder.Body.Len() > 0)
		require.NotContains(t, recorder.Body.String(), "later mutation")
	}
}
