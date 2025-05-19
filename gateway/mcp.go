package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/mcp/extension"
	"github.com/viant/datly/repository"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

func (r *Router) buildToolsIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	component, err := provider.Component(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get component from provider: %w", err)
	}
	toolInputType := r.buildToolInputType(component)

	meta := aPath.Meta.Build(component.View.Name, component.View.Table, &aPath.Path)
	mcpTool := schema.Tool{
		Name:        meta.Name,
		Description: &meta.Description,
		InputSchema: schema.ToolInputSchema{},
	}
	err = mcpTool.InputSchema.Load(reflect.New(toolInputType).Interface())
	if err != nil {
		return err
	}

	handler := r.mcpToolCallHandler(component, aRoute)

	tool := &extension.Tool{
		Tool:    mcpTool,
		Handler: handler,
	}
	r.mcp.AddTool(tool)
	return nil
}

func (r *Router) mcpToolCallHandler(component *repository.Component, aRoute *Route) func(ctx context.Context, params *schema.CallToolRequestParams) (*schema.CallToolResult, error) {
	handler := func(ctx context.Context, params *schema.CallToolRequestParams) (*schema.CallToolResult, error) {
		URI := furl.Path(aRoute.Path.URI)
		URL := fmt.Sprintf("http://localhost/%v", strings.TrimLeft(URI, "/")) // fallback to a local URL for now, this should be replaced with the actual service URL
		values := url.Values{}
		var body io.Reader
		var uniquePath = make(map[string]bool)
		var uniqueQuery = make(map[string]bool)
		for _, parameter := range component.Input.Type.Parameters {

			paramName := strings.Title(parameter.Name)
			value := params.Arguments[paramName]
			switch parameter.In.Kind {
			case state.KindPath:
				if uniquePath[parameter.In.Name] {
					continue
				}
				uniquePath[parameter.In.Name] = true

				if value == nil {
					return nil, jsonrpc.NewInvalidRequest("missing path parameter: "+parameter.In.Name, nil)
				}
				URL = strings.ReplaceAll(URL, "{"+parameter.In.Name+"}", fmt.Sprintf("%v", value))
			case state.KindQuery, state.KindForm:
				if uniqueQuery[parameter.In.Name] {
					continue
				}
				uniqueQuery[parameter.In.Name] = true
				values.Add(parameter.In.Name, fmt.Sprintf("%s", value))
			case state.KindRequestBody:
				if text, ok := value.(string); ok {
					body = strings.NewReader(text)
				} else {
					data, err := json.Marshal(value)
					if err != nil {
						return nil, fmt.Errorf("failed to marshal request body: %w", err)
					}
					body = strings.NewReader(string(data))
				}
			}
		}
		responseWriter := proxy.NewWriter()
		httpRequest, err := http.NewRequest(aRoute.Path.Method, URL, body)
		if err != nil {
			return nil, jsonrpc.NewInvalidRequest(err.Error(), nil)
		}
		r.addAuthTokenIfPresent(ctx, httpRequest)
		httpRequest.RequestURI = httpRequest.URL.RequestURI()
		aRoute.Handle(responseWriter, httpRequest) // route the request to the actual handler
		var result = schema.CallToolResult{}
		mimeType := "application/json"
		result.Content = append(result.Content, schema.CallToolResultContentElem{
			MimeType: mimeType,
			Type:     "text",
			Text:     responseWriter.Body.String(),
		})
		return &result, nil
	}
	return handler
}

func (r *Router) addAuthTokenIfPresent(ctx context.Context, httpRequest *http.Request) {
	if tokenValue := ctx.Value(authorization.TokenKey); tokenValue != nil {
		if token, ok := tokenValue.(*authorization.Token); ok {
			if !strings.HasPrefix(token.Token, "Bearer ") {
				token.Token = "Bearer " + token.Token
			}
			httpRequest.Header.Set("Authorization", fmt.Sprintf("%s", token.Token))
		}
	}
}

func (r *Router) buildToolInputType(components *repository.Component) reflect.Type {
	var inputFields []reflect.StructField
	var uniqueQuery = make(map[string]bool)
	var uniquePath = make(map[string]bool)
	for _, parameter := range components.Input.Type.Parameters {
		name := strings.Title(parameter.Name)
		switch parameter.In.Kind {
		case state.KindPath:
			if uniquePath[parameter.In.Name] {
				continue
			}
			uniquePath[parameter.In.Name] = true
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type()})
		case state.KindQuery, state.KindForm:

			if uniqueQuery[parameter.In.Name] {
				continue
			}
			uniqueQuery[parameter.In.Name] = true
			tag := reflect.StructTag(parameter.Tag)
			if !strings.Contains(parameter.Tag, "required") {
				tag = `json:",omitempty"`
			}
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type(), Tag: tag})
		case state.KindRequestBody:
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type()})
		}
	}

	return reflect.StructOf(inputFields)
}

func (r *Router) buildTemplateResourceIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	var parameterNames []string
	for _, parameter := range item.Resource.Parameters {
		switch parameter.In.Kind {
		case state.KindQuery, state.KindForm:
			parameterNames = append(parameterNames, parameter.In.Name)
		}
	}
	canBuildTemplateResource := len(parameterNames) > 0 || strings.Contains(aPath.URI, "{")
	if !canBuildTemplateResource {
		return nil
	}

	URL := furl.Join("datly://localhost", aPath.URI)
	if len(parameterNames) > 0 {
		// append query parameters to the URL
		URL += "{?" + strings.Join(parameterNames, ",") + "}"
	}
	meta := aPath.Meta.Build(aPath.View.Ref, aPath.View.Ref, &aPath.Path)
	mimeType := "application/json"
	mcpResourceTemplate := schema.ResourceTemplate{
		UriTemplate: URL,
		Name:        meta.Name,
		Description: &meta.Description,
		MimeType:    &mimeType,
	}
	resourceTemplate := &extension.ResourceTemplate{
		ResourceTemplate: mcpResourceTemplate,
		Handler: func(ctx context.Context, params *schema.ReadResourceRequestParams) ([]schema.ReadResourceResultContentsElem, error) {
			return r.handleMcpRead(ctx, params, &mcpResourceTemplate, aRoute, provider)
		},
	}
	// Build the integration for the resource
	r.mcp.AddResourceTemplate(resourceTemplate)
	return nil
}

func (r *Router) buildResourceIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	var parameterNames []string
	for _, parameter := range item.Resource.Parameters {
		switch parameter.In.Kind {
		case state.KindQuery, state.KindForm:
			parameterNames = append(parameterNames, parameter.In.Name)
		}
	}
	hasPathParameter := strings.Contains(aPath.URI, "{")
	if hasPathParameter {
		return nil
	}

	URL := furl.Join("datly://localhost", aPath.URI)
	meta := aPath.Meta.Build(aPath.View.Ref, aPath.View.Ref, &aPath.Path)
	mimeType := "application/json"
	mcpResource := schema.Resource{
		Uri:         URL,
		Name:        meta.Name,
		Description: &meta.Description,
		MimeType:    &mimeType,
	}
	mcpResourceTemplate := schema.ResourceTemplate{
		UriTemplate: URL,
		Name:        meta.Name,
		Description: &meta.Description,
		MimeType:    &mimeType,
	}
	resourceTemplate := &extension.Resource{
		Resource: mcpResource,
		Handler: func(ctx context.Context, params *schema.ReadResourceRequestParams) ([]schema.ReadResourceResultContentsElem, error) {
			return r.handleMcpRead(ctx, params, &mcpResourceTemplate, aRoute, provider)
		},
	}
	// Build the integration for the resource
	r.mcp.AddResource(resourceTemplate)
	return nil
}

func (r *Router) handleMcpRead(ctx context.Context, params *schema.ReadResourceRequestParams, template *schema.ResourceTemplate, aRoute *Route, provider *repository.Provider) ([]schema.ReadResourceResultContentsElem, error) {
	URI := furl.Path(params.Uri)
	URL := fmt.Sprintf("http://localhost/%v", URI) // fallback to a local URL for now, this should be replaced with the actual service URL
	component, err := provider.Component(ctx)      // ensure the provider is initialized
	if err != nil {
		return nil, fmt.Errorf("failed to get component from provider: %w", err)
	}
	byLoc := make(map[string]*state.Parameter)
	for _, param := range component.View.GetResource().Parameters {
		byLoc[param.In.Name] = param
	}

	responseWriter := proxy.NewWriter()
	httpRequest, err := http.NewRequest(http.MethodGet, URL, nil)
	if err != nil {
		return nil, jsonrpc.NewInvalidRequest(err.Error(), nil)
	}
	r.addAuthTokenIfPresent(ctx, httpRequest)
	aRoute.Handle(responseWriter, httpRequest) // route the request to the actual handler
	var result []schema.ReadResourceResultContentsElem
	mimeType := ""
	if template.MimeType != nil {
		mimeType = *template.MimeType
	}
	result = append(result, schema.ReadResourceResultContentsElem{
		Uri:      URL,                          // The URI of the resource
		Text:     responseWriter.Body.String(), // The actual data returned from the request
		MimeType: &mimeType,                    // The MIME type of the resource
		Blob:     responseWriter.Body.String(),
	})
	return result, nil

}
