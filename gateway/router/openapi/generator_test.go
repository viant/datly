package openapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	openapi3 "github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestGeneratorTopLevel_Table(t *testing.T) {
	ctx := context.Background()
	info := openapi3.Info{Title: "api", Version: "1"}

	t.Run("generate spec no providers", func(t *testing.T) {
		g := &generator{_schemasIndex: map[string]*openapi3.Schema{}, commonParameters: map[string]*openapi3.Parameter{}, _parametersIndex: map[string]*openapi3.Parameter{}}
		spec, err := g.GenerateSpec(ctx, &repository.Service{}, info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec == nil || spec.OpenAPI != "3.0.1" {
			t.Fatalf("unexpected spec")
		}
	})

	t.Run("wrapper generate no providers", func(t *testing.T) {
		spec, err := GenerateOpenAPI3Spec(ctx, &repository.Service{}, info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec == nil || spec.Info == nil || spec.Info.Title != "api" {
			t.Fatalf("unexpected wrapper result")
		}
	})

	t.Run("generate paths no providers", func(t *testing.T) {
		g := &generator{}
		schemas, paths, err := g.generatePaths(ctx, &repository.Service{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if schemas == nil || len(paths) != 0 {
			t.Fatalf("unexpected result")
		}
	})

	t.Run("marshal generated spec response keys", func(t *testing.T) {
		control := &version.Control{}
		comp := newTestComponent(t)
		comp.Method = http.MethodGet
		comp.Path.Method = http.MethodGet
		comp.Path.URI = "/v1/spec"
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		provider := repository.NewProvider(comp.Path, control, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) { return comp, nil })

		spec, err := GenerateOpenAPI3Spec(ctx, &repository.Service{}, info, provider)
		if err != nil {
			t.Fatalf("unexpected spec generation error: %v", err)
		}
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatalf("unexpected marshal error: %v", err)
		}
		doc := string(data)
		if !strings.Contains(doc, `"responses":{"`+string(openapi3.ResponseOK)+`":`) {
			t.Fatalf("expected serialized numeric response key as string in spec: %s", doc)
		}
		if !strings.Contains(doc, `"default"`) {
			t.Fatalf("expected default response key in spec: %s", doc)
		}
	})
}

func TestAttachOperation_Table(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		assertion func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation)
	}{
		{
			name:   "get",
			method: http.MethodGet,
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Get != op {
					t.Fatalf("expected get operation")
				}
			},
		},
		{
			name:   "post",
			method: http.MethodPost,
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Post != op {
					t.Fatalf("expected post operation")
				}
			},
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Delete != op {
					t.Fatalf("expected delete operation")
				}
			},
		},
		{
			name:   "put",
			method: http.MethodPut,
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Put != op {
					t.Fatalf("expected put operation")
				}
			},
		},
		{
			name:   "patch",
			method: http.MethodPatch,
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Patch != op {
					t.Fatalf("expected patch operation")
				}
			},
		},
		{
			name:   "unsupported",
			method: "TRACE",
			assertion: func(t *testing.T, item *openapi3.PathItem, op *openapi3.Operation) {
				if item.Get != nil || item.Post != nil || item.Delete != nil || item.Put != nil || item.Patch != nil {
					t.Fatalf("did not expect any method to be set")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &openapi3.PathItem{}
			op := &openapi3.Operation{}
			attachOperation(item, tt.method, op)
			tt.assertion(t, item, op)
		})
	}
}

func TestGeneratorHelpersMore_Table(t *testing.T) {
	g := &generator{}

	t.Run("view parameters empty", func(t *testing.T) {
		comp := &ComponentSchema{component: &repository.Component{}, schemas: NewContainer()}
		v := &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		params, err := g.viewParameters(context.Background(), v, comp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(params) != 0 {
			t.Fatalf("expected no params")
		}
	})

	t.Run("get all views params empty with relation", func(t *testing.T) {
		comp := &ComponentSchema{component: &repository.Component{}, schemas: NewContainer()}
		v := &view.View{Template: &view.Template{}, Selector: &view.Config{}, With: []*view.Relation{{Of: &view.ReferenceView{View: view.View{Template: &view.Template{}, Selector: &view.Config{}}}}}}
		params, err := g.getAllViewsParameters(context.Background(), comp, v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(params) != 0 {
			t.Fatalf("expected no params")
		}
	})

	t.Run("append built-in nil", func(t *testing.T) {
		comp := &ComponentSchema{component: &repository.Component{}, schemas: NewContainer()}
		params := []*openapi3.Parameter{}
		if err := g.appendBuiltInParam(context.Background(), &params, comp, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("request body nil for get", func(t *testing.T) {
		comp := &ComponentSchema{component: &repository.Component{Path: repository.Component{}.Path}, schemas: NewContainer()}
		comp.component.Path.Method = http.MethodGet
		body, err := g.requestBody(context.Background(), comp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if body != nil {
			t.Fatalf("expected nil body")
		}
	})

	t.Run("responses nil for options", func(t *testing.T) {
		comp := &ComponentSchema{component: &repository.Component{}, schemas: NewContainer()}
		comp.component.Method = http.MethodOptions
		resp, err := g.responses(context.Background(), comp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp == nil {
			t.Fatalf("expected non-nil response map")
		}
		if len(resp) != 0 {
			t.Fatalf("expected empty response map for options")
		}
	})

	t.Run("request body for post", func(t *testing.T) {
		comp := newTestComponent(t)
		comp.Path.Method = http.MethodPost
		comp.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		body, err := g.requestBody(context.Background(), cSchema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if body == nil || body.Content[ApplicationJson] == nil {
			t.Fatalf("expected request body")
		}
	})

	t.Run("responses success and default", func(t *testing.T) {
		comp := newTestComponent(t)
		comp.Method = http.MethodGet
		comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		resp, err := g.responses(context.Background(), cSchema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := openapi3.GetResponse(resp, openapi3.ResponseOK); !ok {
			t.Fatalf("expected success response")
		}
		if _, ok := openapi3.GetResponse(resp, openapi3.ResponseDefault); !ok {
			t.Fatalf("expected standard responses")
		}
	})

	t.Run("convert param query", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		param := &state.Parameter{Name: "ID", In: &state.Location{Kind: state.KindQuery}, Schema: state.NewSchema(reflect.TypeOf(1))}
		converted, ok, err := g.convertParam(context.Background(), cSchema, param, "")
		if err != nil || !ok || len(converted) != 1 {
			t.Fatalf("unexpected convert result: %v %v %d", ok, err, len(converted))
		}
	})

	t.Run("convert param object and non-http", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}

		objectParam := &state.Parameter{
			Name: "Obj",
			In:   &state.Location{Kind: state.KindObject},
			Object: state.Parameters{
				{Name: "A", In: state.NewQueryLocation("a"), Schema: state.NewSchema(reflect.TypeOf(""))},
			},
		}
		converted, ok, err := g.convertParam(context.Background(), cSchema, objectParam, "")
		if err != nil || !ok || len(converted) != 1 {
			t.Fatalf("unexpected object convert: %v %v %d", ok, err, len(converted))
		}

		nonHTTP := &state.Parameter{Name: "S", In: &state.Location{Kind: state.KindState, Name: "state"}, Schema: state.NewSchema(reflect.TypeOf(""))}
		converted, ok, err = g.convertParam(context.Background(), cSchema, nonHTTP, "")
		if err != nil || ok || len(converted) != 0 {
			t.Fatalf("unexpected non-http convert: %v %v %d", ok, err, len(converted))
		}
	})

	t.Run("convert param via kind param and cache ref", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		base := &state.Parameter{Name: "ID", In: state.NewQueryLocation("id"), Schema: state.NewSchema(reflect.TypeOf(1))}
		comp.Input.Type.Parameters = state.Parameters{base}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}

		refParam := &state.Parameter{Name: "Ref", In: &state.Location{Kind: state.KindParam, Name: "ID"}, Schema: state.NewSchema(reflect.TypeOf(1))}
		converted, ok, err := g.convertParam(context.Background(), cSchema, refParam, "")
		if err != nil || !ok || len(converted) != 1 {
			t.Fatalf("unexpected kind-param convert: %v %v %d", ok, err, len(converted))
		}

		converted, ok, err = g.convertParam(context.Background(), cSchema, base, "")
		if err != nil || !ok || len(converted) != 1 {
			t.Fatalf("unexpected cache convert: %v %v %#v", ok, err, converted)
		}
	})

	t.Run("append built-in and view params", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		params := []*openapi3.Parameter{}
		param := &state.Parameter{Name: "Limit", In: state.NewQueryLocation("limit"), Schema: state.NewSchema(reflect.TypeOf(1))}
		if err := g.appendBuiltInParam(context.Background(), &params, cSchema, param); err != nil {
			t.Fatalf("unexpected append error: %v", err)
		}
		if len(params) == 0 {
			t.Fatalf("expected built-in param")
		}
	})

	t.Run("view parameters with selector built-ins", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		v := &view.View{
			Template: &view.Template{
				Parameters: state.Parameters{
					{Name: "Q", In: state.NewQueryLocation("q"), Schema: state.NewSchema(reflect.TypeOf(""))},
				},
			},
			Selector: &view.Config{
				CriteriaParameter: &state.Parameter{Name: "Criteria", In: state.NewQueryLocation("_criteria"), Schema: state.NewSchema(reflect.TypeOf(""))},
				LimitParameter:    &state.Parameter{Name: "Limit", In: state.NewQueryLocation("_limit"), Schema: state.NewSchema(reflect.TypeOf(1))},
				OffsetParameter:   &state.Parameter{Name: "Offset", In: state.NewQueryLocation("_offset"), Schema: state.NewSchema(reflect.TypeOf(1))},
				PageParameter:     &state.Parameter{Name: "Page", In: state.NewQueryLocation("_page"), Schema: state.NewSchema(reflect.TypeOf(1))},
				OrderByParameter:  &state.Parameter{Name: "OrderBy", In: state.NewQueryLocation("_orderby"), Schema: state.NewSchema(reflect.TypeOf([]string{}))},
				FieldsParameter:   &state.Parameter{Name: "Fields", In: state.NewQueryLocation("_fields"), Schema: state.NewSchema(reflect.TypeOf([]string{}))},
			},
		}
		params, err := g.viewParameters(context.Background(), v, cSchema)
		if err != nil {
			t.Fatalf("unexpected viewParameters error: %v", err)
		}
		if len(params) < 7 {
			t.Fatalf("expected builtin and template params, got %d", len(params))
		}

		v.Template.Parameters = append(v.Template.Parameters, &state.Parameter{Name: "StateParam", In: &state.Location{Kind: state.KindState, Name: "s"}, Schema: state.NewSchema(reflect.TypeOf(""))})
		params, err = g.viewParameters(context.Background(), v, cSchema)
		if err != nil {
			t.Fatalf("unexpected viewParameters error: %v", err)
		}
		if len(params) < 7 {
			t.Fatalf("expected params with non-http skipped, got %d", len(params))
		}
	})

	t.Run("generate operation happy path", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.Method = http.MethodPost
		comp.Path.Method = http.MethodPost
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		operation, err := g.generateOperation(context.Background(), cSchema)
		if err != nil || operation == nil {
			t.Fatalf("unexpected operation result: %v %v", operation, err)
		}
		if _, ok := openapi3.GetResponse(operation.Responses, openapi3.ResponseOK); !ok {
			t.Fatalf("expected 200 response")
		}
	})

	t.Run("generate operation with component parameter", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}

		components := &repository.Service{}
		registry := repository.NewRegistry("", nil, nil)
		setUnexportedField(components, "registry", registry)

		dep := newTestComponent(t)
		dep.Method = http.MethodGet
		dep.Path.Method = http.MethodGet
		dep.Path.URI = "/v1/dep"
		dep.View = &view.View{
			Template: &view.Template{
				Parameters: state.Parameters{
					{Name: "DepID", In: state.NewQueryLocation("depId"), Schema: state.NewSchema(reflect.TypeOf(1))},
				},
			},
			Selector: &view.Config{},
		}
		dep.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		registry.Register(dep)

		comp := newTestComponent(t)
		comp.Method = http.MethodPost
		comp.Path.Method = http.MethodPost
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp.Output.Type = state.Type{
			Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{})),
			Parameters: state.Parameters{
				{Name: "Dep", In: &state.Location{Kind: state.KindComponent, Name: "GET:/v1/dep"}},
			},
		}

		cSchema := &ComponentSchema{component: comp, components: components, schemas: NewContainer()}
		operation, err := g.generateOperation(context.Background(), cSchema)
		if err != nil {
			t.Fatalf("unexpected operation error: %v", err)
		}
		if operation == nil || len(operation.Parameters) == 0 {
			t.Fatalf("expected operation with merged parameters")
		}
	})

	t.Run("generate operation request body error", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.Method = http.MethodPost
		comp.Path.Method = http.MethodPost
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf((chan int)(nil)))}
		comp.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf((chan int)(nil)))}
		comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		if _, err := g.generateOperation(context.Background(), cSchema); err == nil {
			t.Fatalf("expected request body generation error")
		}
	})

	t.Run("generate operation response error", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.Method = http.MethodGet
		comp.Path.Method = http.MethodGet
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf((chan int)(nil)))}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		if _, err := g.generateOperation(context.Background(), cSchema); err == nil {
			t.Fatalf("expected response generation error")
		}
	})

	t.Run("generate paths with providers", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		control := &version.Control{}
		comp1 := newTestComponent(t)
		comp1.Method = http.MethodGet
		comp1.Path.Method = http.MethodGet
		comp1.Path.URI = "/v1/get"
		comp1.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp1.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}

		comp2 := newTestComponent(t)
		comp2.Method = http.MethodPost
		comp2.Path.Method = http.MethodPost
		comp2.Path.URI = "/v1/post"
		comp2.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp2.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp2.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
		comp2.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}

		provider1 := repository.NewProvider(comp1.Path, control, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) { return comp1, nil })
		provider2 := repository.NewProvider(comp2.Path, control, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) { return comp2, nil })

		_, paths, err := g.generatePaths(context.Background(), &repository.Service{}, []*repository.Provider{provider1, provider2})
		if err != nil {
			t.Fatalf("unexpected generate paths error: %v", err)
		}
		if paths["/v1/get"] == nil || paths["/v1/post"] == nil {
			t.Fatalf("expected generated paths")
		}
		if paths["/v1/get"].Get == nil || paths["/v1/get"].Post != nil {
			t.Fatalf("expected isolated GET path item")
		}
		if paths["/v1/post"].Post == nil || paths["/v1/post"].Get != nil {
			t.Fatalf("expected isolated POST path item")
		}
	})

	t.Run("generate paths with all methods and provider errors", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		control := &version.Control{}

		mk := func(method, uri string) *repository.Provider {
			comp := newTestComponent(t)
			comp.Method = method
			comp.Path.Method = method
			comp.Path.URI = uri
			comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
			comp.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
			if method != http.MethodGet {
				comp.Input.Body = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
				comp.Input.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ Name string }{}))}
			}
			return repository.NewProvider(comp.Path, control, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) { return comp, nil })
		}

		errProvider := repository.NewProvider(contract.Path{Method: http.MethodGet, URI: "/v1/error"}, control, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			return nil, errors.New("provider error")
		})

		controlDeleted := &version.Control{}
		controlDeleted.SetChangeKind(version.ChangeKindDeleted)
		nilProvider := repository.NewProvider(contract.Path{Method: http.MethodGet, URI: "/v1/nil"}, controlDeleted, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			return nil, nil
		})

		providers := []*repository.Provider{
			mk(http.MethodDelete, "/v1/delete"),
			mk(http.MethodPut, "/v1/put"),
			mk(http.MethodPatch, "/v1/patch"),
			errProvider,
			nilProvider,
		}
		_, paths, err := g.generatePaths(context.Background(), &repository.Service{}, providers)
		if err == nil {
			t.Fatalf("expected provider error")
		}
		if paths["/v1/delete"] == nil || paths["/v1/put"] == nil || paths["/v1/patch"] == nil {
			t.Fatalf("expected generated method paths")
		}
	})

	t.Run("operation parameters include component output params", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}

		components := &repository.Service{}
		registry := repository.NewRegistry("", nil, nil)
		setUnexportedField(components, "registry", registry)

		dep := newTestComponent(t)
		dep.Method = http.MethodGet
		dep.Path.Method = http.MethodGet
		dep.Path.URI = "/v1/opdep"
		dep.View = &view.View{
			Template: &view.Template{
				Parameters: state.Parameters{
					{Name: "DepID", In: state.NewQueryLocation("depId"), Schema: state.NewSchema(reflect.TypeOf(1))},
				},
			},
			Selector: &view.Config{},
		}
		dep.Output.Type = state.Type{Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{}))}
		registry.Register(dep)

		comp := newTestComponent(t)
		comp.View = &view.View{
			Template: &view.Template{
				Parameters: state.Parameters{
					{Name: "RootQ", In: state.NewQueryLocation("q"), Schema: state.NewSchema(reflect.TypeOf(""))},
				},
			},
			Selector: &view.Config{},
		}
		comp.Output.Type = state.Type{
			Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{})),
			Parameters: state.Parameters{
				{Name: "Dep", In: &state.Location{Kind: state.KindComponent, Name: "GET:/v1/opdep"}},
			},
		}

		cSchema := &ComponentSchema{component: comp, components: components, schemas: NewContainer()}
		params, err := g.operationParameters(context.Background(), cSchema)
		if err != nil {
			t.Fatalf("unexpected operationParameters error: %v", err)
		}
		if len(params) < 2 {
			t.Fatalf("expected root and component params, got %d", len(params))
		}
	})

	t.Run("component output parameters no component refs", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.Output.Type = state.Type{
			Schema:     state.NewSchema(reflect.TypeOf(struct{ ID int }{})),
			Parameters: state.Parameters{{Name: "OnlyState", In: &state.Location{Kind: state.KindState, Name: "s"}}},
		}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		params, err := g.componentOutputParameters(context.Background(), cSchema)
		if err != nil {
			t.Fatalf("unexpected componentOutputParameters error: %v", err)
		}
		if len(params) != 0 {
			t.Fatalf("expected no component params, got %d", len(params))
		}
	})

	t.Run("lookup component param error", func(t *testing.T) {
		g := &generator{}
		components := &repository.Service{}
		registry := repository.NewRegistry("", nil, nil)
		setUnexportedField(components, "registry", registry)
		dep := newTestComponent(t)
		dep.Method = http.MethodGet
		dep.Path.Method = http.MethodGet
		dep.Path.URI = "/v1/existing"
		registry.Register(dep)
		comp := newTestComponent(t)
		cSchema := &ComponentSchema{component: comp, components: components, schemas: NewContainer()}
		if _, err := g.lookupComponentParam(context.Background(), cSchema, "GET:/v1/missing"); err == nil {
			t.Fatalf("expected missing provider error")
		}
	})

	t.Run("operation parameters missing component provider", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		components := &repository.Service{}
		registry := repository.NewRegistry("", nil, nil)
		setUnexportedField(components, "registry", registry)
		existing := newTestComponent(t)
		existing.Method = http.MethodGet
		existing.Path.Method = http.MethodGet
		existing.Path.URI = "/v1/existing"
		registry.Register(existing)

		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		comp.Output.Type = state.Type{
			Schema: state.NewSchema(reflect.TypeOf(struct{ ID int }{})),
			Parameters: state.Parameters{
				{Name: "MissingDep", In: &state.Location{Kind: state.KindComponent, Name: "GET:/v1/unknown"}},
			},
		}
		cSchema := &ComponentSchema{component: comp, components: components, schemas: NewContainer()}
		if _, err := g.operationParameters(context.Background(), cSchema); err == nil {
			t.Fatalf("expected missing dependency error")
		}
	})

	t.Run("convert param cache nil ref", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		param := &state.Parameter{Name: "ID", In: &state.Location{Kind: state.KindQuery}, Schema: state.NewSchema(reflect.TypeOf(1))}

		first, ok, err := g.convertParam(context.Background(), cSchema, param, "")
		if err != nil || !ok || len(first) != 1 {
			t.Fatalf("unexpected first convert result: %v %v %d", ok, err, len(first))
		}

		second, ok, err := g.convertParam(context.Background(), cSchema, param, "")
		if err != nil || !ok || len(second) != 1 {
			t.Fatalf("unexpected second convert result: %v %v %d", ok, err, len(second))
		}
		if second[0].Ref == "" {
			t.Fatalf("expected parameter ref")
		}

		third, ok, err := g.convertParam(context.Background(), cSchema, param, "")
		if err != nil || !ok || len(third) != 1 {
			t.Fatalf("unexpected third convert result: %v %v %d", ok, err, len(third))
		}
		if third[0].Ref == "" {
			t.Fatalf("expected cached nil path to still return ref")
		}
	})

	t.Run("append built-in non-http parameter", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}
		params := []*openapi3.Parameter{}
		stateParam := &state.Parameter{Name: "StateOnly", In: &state.Location{Kind: state.KindState, Name: "state"}, Schema: state.NewSchema(reflect.TypeOf(""))}
		if err := g.appendBuiltInParam(context.Background(), &params, cSchema, stateParam); err != nil {
			t.Fatalf("unexpected append error: %v", err)
		}
		if len(params) != 0 {
			t.Fatalf("expected non-http built-in param to be skipped")
		}
	})

	t.Run("view parameters and relation errors", func(t *testing.T) {
		g := &generator{
			_parametersIndex: map[string]*openapi3.Parameter{},
			commonParameters: map[string]*openapi3.Parameter{},
		}
		comp := newTestComponent(t)
		comp.View = &view.View{Template: &view.Template{}, Selector: &view.Config{}}
		cSchema := &ComponentSchema{component: comp, schemas: NewContainer()}

		errorView := &view.View{
			Template: &view.Template{
				Parameters: state.Parameters{
					{Name: "Bad", In: state.NewQueryLocation("bad"), Schema: state.NewSchema(reflect.TypeOf((chan int)(nil)))},
				},
			},
			Selector: &view.Config{},
		}
		if _, err := g.viewParameters(context.Background(), errorView, cSchema); err == nil {
			t.Fatalf("expected view parameter conversion error")
		}

		relationErrorView := &view.View{
			Template: &view.Template{},
			Selector: &view.Config{},
			With: []*view.Relation{
				{Of: &view.ReferenceView{View: *errorView}},
			},
		}
		if _, err := g.getAllViewsParameters(context.Background(), cSchema, relationErrorView); err == nil {
			t.Fatalf("expected relation parameter conversion error")
		}
	})
}
