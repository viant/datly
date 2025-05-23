package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/repository"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/toolbox"
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
		Name:        strings.ReplaceAll(meta.Name, " ", ""),
		Description: &meta.Description,
		InputSchema: schema.ToolInputSchema{},
	}
	if _, ok := r.mcpRegistry.ToolRegistry.Get(mcpTool.Name); ok {
		return nil //already registered
	}
	err = mcpTool.InputSchema.Load(reflect.New(toolInputType).Interface())
	if err != nil {
		return err
	}
	handler := r.mcpToolCallHandler(component, aRoute)
	tool := &serverproto.ToolEntry{
		Metadata: mcpTool,
		Handler:  handler,
	}
	r.mcpRegistry.RegisterTool(tool)
	return nil
}

func (r *Router) mcpToolCallHandler(component *repository.Component, aRoute *Route) serverproto.ToolHandlerFunc {
	handler := func(ctx context.Context, req *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		params := req.Params
		URI := r.matchToolCallComponentURI(aRoute, component, params)

		URL := fmt.Sprintf("http://localhost/%v", strings.TrimLeft(URI, "/")) // fallback to a local URL for now, this should be replaced with the actual service URL
		values := url.Values{}
		var body io.Reader
		var uniquePath = make(map[string]bool)
		var uniqueQuery = make(map[string]bool)

		for _, parameter := range component.Input.Type.Parameters {

			paramName := strings.Title(parameter.Name)
			value := params.Arguments[paramName]
			paramType := parameter.Schema.Type()
			if paramType.Kind() == reflect.Ptr {
				paramType = paramType.Elem()
			}

			switch paramType.Kind() {
			case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
				value = toolbox.AsInt(value)
			}

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
						return nil, jsonrpc.NewInvalidParamsError("failed to marshal request body: %w", data)
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
		if URI != aRoute.URI() {
			if matchedRoute, _ := r.match(component.Method, URI, httpRequest); matchedRoute != nil {
				aRoute = matchedRoute
			}
		}

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

func (r *Router) matchToolCallComponentURI(aRoute *Route, component *repository.Component, params schema.CallToolRequestParams) string {
	URI := furl.Path(aRoute.Path.URI)
	for _, parameter := range component.Input.Type.Parameters {
		if parameter.URI == "" {
			continue
		}
		paramName := strings.Title(parameter.Name)
		value, ok := params.Arguments[paramName]
		if !ok || value == nil || value == "" {
			continue
		}
		URI = furl.Path(parameter.URI)
		break
	}
	return URI
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
		Name:        strings.ReplaceAll(meta.Name, " ", ""),
		Description: &meta.Description,
		MimeType:    &mimeType,
	}

	// Check if the resource template is already registered
	handler := r.reactMcpResourceHandler(mcpResourceTemplate, aRoute, provider)
	if r.hasMcpResource(mcpResourceTemplate.UriTemplate) {
		return nil
	}

	// Build the integration for the resource
	r.mcpRegistry.RegisterResourceTemplate(mcpResourceTemplate, handler)
	return nil
}

func (r *Router) reactMcpResourceHandler(mcpResourceTemplate schema.ResourceTemplate, aRoute *Route, provider *repository.Provider) func(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	handler := func(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
		result, err := r.handleMcpRead(ctx, &request.Params, &mcpResourceTemplate, aRoute, provider)
		if err != nil {
			return nil, jsonrpc.NewInternalError(err.Error(), nil)
		}
		if len(result) == 0 {
			return &schema.ReadResourceResult{Contents: []schema.ReadResourceResultContentsElem{}}, nil
		}
		return &schema.ReadResourceResult{Contents: result}, nil
	}
	return handler
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
		Name:        strings.ReplaceAll(meta.Name, " ", ""),
		Description: &meta.Description,
		MimeType:    &mimeType,
	}
	mcpResourceTemplate := schema.ResourceTemplate{
		UriTemplate: URL,
		Name:        meta.Name,
		Description: &meta.Description,
		MimeType:    &mimeType,
	}

	// Check if the resource template is already registered
	handler := r.reactMcpResourceHandler(mcpResourceTemplate, aRoute, provider)
	if r.hasMcpResource(mcpResourceTemplate.UriTemplate) {
		return nil
	}
	// Build the integration for the mcpResource
	r.mcpRegistry.RegisterResource(mcpResource, handler)
	return nil
}

func (r *Router) hasMcpResource(URI string) bool {
	if _, ok := r.mcpRegistry.ResourceRegistry.Get(URI); ok {
		return true //already registered
	}
	if _, ok := r.mcpRegistry.ResourceTemplateRegistry.Get(URI); ok {
		return true //already registered
	}
	return false
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
