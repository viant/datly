package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	furl "github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/repository"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/toolbox"
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
	return func(ctx context.Context, req *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		params := req.Params
		uri := r.matchToolCallComponentURI(aRoute, component, params)
		baseURL := fmt.Sprintf("http://localhost/%v", strings.TrimLeft(uri, "/")) // replace with actual service URL when available

		values := url.Values{}
		var body io.Reader
		uniquePath := map[string]bool{}
		uniqueQuery := map[string]bool{}

		// 1) Collect parameters (component + selector pagination)
		allParams := r.collectToolParameters(component)

		// 2) Apply parameters to request URL/query/body
		for _, p := range allParams {
			name := strings.Title(p.Name)
			value := params.Arguments[name]
			pType := p.Schema.Type()
			if pType.Kind() == reflect.Ptr {
				pType = pType.Elem()
			}
			value = r.coerceNumericValue(value, pType)
			var rpcErr *jsonrpc.Error
			baseURL, body, rpcErr = r.applyParamToRequest(baseURL, values, p, value, uniquePath, uniqueQuery, body)
			if rpcErr != nil {
				return nil, rpcErr
			}
		}

		// 3) Finalize URL with query string
		finalURL := baseURL
		if enc := values.Encode(); enc != "" {
			if strings.Contains(finalURL, "?") {
				finalURL += "&" + enc
			} else {
				finalURL += "?" + enc
			}
		}

		// 4) Build HTTP request and route
		httpReq, rpcErr := r.newToolHTTPRequest(aRoute.Path.Method, finalURL, body)
		if rpcErr != nil {
			return nil, rpcErr
		}
		r.addAuthTokenIfPresent(ctx, httpReq)

		// NEW: map MCP view sync flag argument to Sync-Read header
		r.addSyncReadHeaderIfPresent(ctx, component, &params, httpReq)

		httpReq.RequestURI = httpReq.URL.RequestURI()
		if uri != aRoute.URI() {
			if matched, _ := r.match(component.Method, uri, httpReq); matched != nil {
				aRoute = matched
			}
		}
		rw := proxy.NewWriter()
		aRoute.Handle(rw, httpReq)

		if rw.Code == http.StatusUnauthorized {
			return nil, r.mcpUnauthorizedError()
		}

		// 5) Build tool result (text + structured on error)
		return r.buildToolCallResult(rw, finalURL, aRoute.Path.Method), nil
	}
}

func (r *Router) addSyncReadHeaderIfPresent(
	ctx context.Context,
	component *repository.Component,
	params *schema.CallToolRequestParams,
	httpRequest *http.Request,
) {
	if params == nil || params.Arguments == nil {
		return
	}
	// MCP tool arguments are generated using exported Go field names, so
	// the Datly view sync flag (view.SyncFlag == "viewSyncFlag") will appear
	// as "viewSyncFlag" in the schema/tool call.
	const mcpSyncFlagArg = "viewSyncFlag"
	const headerName = "Sync-Read"

	value, ok := params.Arguments[mcpSyncFlagArg]
	if !ok {
		return
	}

	if !isTruthy(value) {
		return
	}

	// Optionally, ensure that the underlying component actually declares
	// a sync flag parameter; if it does not, we simply skip setting the header.
	if !hasSyncFlagParameter(component) {
		return
	}

	httpRequest.Header.Set(headerName, "true")
}

// hasSyncFlagParameter checks whether the component declares a selector
// sync flag parameter, which should be exposed as view.SyncFlag.
func hasSyncFlagParameter(component *repository.Component) bool {
	if component == nil || component.View == nil || component.View.Selector == nil {
		return false
	}
	param := component.View.Selector.GetSyncFlagParameter()
	if param == nil {
		return false
	}
	// The selector sync flag parameter is defined in view.Config using
	// view.SyncFlag as the state key, but here we simply check that it exists.
	return true
}

// isTruthy interprets common JSON-serialised truthy values.
func isTruthy(v interface{}) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		s := strings.TrimSpace(strings.ToLower(value))
		return s == "true" || s == "1" || s == "yes" || s == "y"
	case float64:
		return value != 0
	default:
		return false
	}
}

// collectToolParameters aggregates component input parameters with selector pagination (limit/offset) when available.
func (r *Router) collectToolParameters(component *repository.Component) []*state.Parameter {
	var all []*state.Parameter
	all = append(all, component.Input.Type.Parameters...)
	if component.View != nil && component.View.Selector != nil {
		if p := component.View.Selector.LimitParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.OffsetParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.FieldsParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.PageParameter; p != nil {
			all = append(all, p)
		}
	}
	return all
}

// coerceNumericValue normalizes numeric values to integers when appropriate.
func (r *Router) coerceNumericValue(value interface{}, paramType reflect.Type) interface{} {
	switch paramType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Float64:
		if value == nil {
			return nil
		}
		return toolbox.AsInt(value)
	}
	return value
}

// applyParamToRequest applies a single parameter into path placeholders, query/form values, or request body.
func (r *Router) applyParamToRequest(baseURL string, values url.Values, p *state.Parameter, value interface{}, uniquePath, uniqueQuery map[string]bool, body io.Reader) (string, io.Reader, *jsonrpc.Error) {
	switch p.In.Kind {
	case state.KindPath:
		if uniquePath[p.In.Name] {
			return baseURL, body, nil
		}
		uniquePath[p.In.Name] = true
		if value == nil {
			// If parameter has its own URI segment configured, treat as optional and strip the placeholder.
			if p.URI != "" {
				baseURL = strings.ReplaceAll(baseURL, "/{"+p.In.Name+"}", "")
				baseURL = strings.ReplaceAll(baseURL, "{"+p.In.Name+"}", "")
				return baseURL, body, nil
			}
			return baseURL, body, jsonrpc.NewInvalidRequest("missing path parameter: "+p.In.Name, nil)
		}
		baseURL = strings.ReplaceAll(baseURL, "{"+p.In.Name+"}", fmt.Sprintf("%v", value))
	case state.KindQuery, state.KindForm:
		if uniqueQuery[p.In.Name] {
			return baseURL, body, nil
		}
		uniqueQuery[p.In.Name] = true
		if value == nil || value == "" {
			return baseURL, body, nil
		}
		if slice, ok := value.([]interface{}); ok {
			var items []string
			for _, item := range slice {
				if f, ok := item.(float64); ok {
					items = append(items, fmt.Sprintf("%v", int64(f)))
				} else {
					items = append(items, fmt.Sprintf("%v", item))
				}
			}
			values.Add(p.In.Name, strings.Join(items, ","))
		} else {
			values.Add(p.In.Name, fmt.Sprintf("%v", value))
		}
	case state.KindRequestBody:
		if text, ok := value.(string); ok {
			body = strings.NewReader(text)
		} else {
			data, err := json.Marshal(value)
			if err != nil {
				return baseURL, body, jsonrpc.NewInvalidParamsError("failed to marshal request body", nil)
			}
			body = strings.NewReader(string(data))
		}
	}
	return baseURL, body, nil
}

// newToolHTTPRequest constructs an HTTP request for routed tool invocation.
func (r *Router) newToolHTTPRequest(method, URL string, body io.Reader) (*http.Request, *jsonrpc.Error) {
	httpRequest, err := http.NewRequest(method, URL, body)
	if err != nil {
		return nil, jsonrpc.NewInvalidRequest(err.Error(), nil)
	}
	return httpRequest, nil
}

// buildToolCallResult composes a CallToolResult with text content and structured error info if status is not OK.
func (r *Router) buildToolCallResult(responseWriter *proxy.Writer, URL, method string) *schema.CallToolResult {
	var result = &schema.CallToolResult{}
	mimeType := responseWriter.HeaderMap.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/json"
	}
	data := responseWriter.Body.Bytes()
	result.Content = append(result.Content, schema.CallToolResultContentElem{
		MimeType: mimeType,
		Type:     "text",
		Text:     string(data),
	})
	_ = json.Unmarshal(data, &result.StructuredContent)
	if responseWriter.Code >= http.StatusBadRequest {
		isErr := true
		result.IsError = &isErr
		result.StructuredContent = map[string]interface{}{
			"status":  responseWriter.Code,
			"error":   true,
			"message": responseWriter.Body.String(),
			"headers": responseWriter.HeaderMap,
			"uri":     URL,
			"method":  method,
		}
	}
	return result
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

const defaultMCPProtectedResource = "https://datly.viantinc.com"

func (r *Router) mcpUnauthorizedError() *jsonrpc.Error {
	if r == nil || r.config == nil || r.config.MCP == nil {
		return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", nil)
	}
	issuerURL := strings.TrimSpace(r.config.MCP.IssuerURL)
	if issuerURL == "" {
		return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", nil)
	}
	return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", &authorization.Authorization{
		RequiredScopes: []string{},
		UseIdToken:     true,
		ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{
			Resource:             defaultMCPProtectedResource,
			AuthorizationServers: []string{issuerURL},
		},
	})
}

func (r *Router) buildToolInputType(components *repository.Component) reflect.Type {
	var inputFields []reflect.StructField
	var uniqueQuery = make(map[string]bool)
	var uniquePath = make(map[string]bool)
	// Include component input parameters
	for _, parameter := range components.Input.Type.Parameters {
		name := strings.Title(parameter.Name)
		switch parameter.In.Kind {
		case state.KindPath:
			if uniquePath[parameter.In.Name] {
				continue
			}
			uniquePath[parameter.In.Name] = true
			// If parameter is a slice, make it optional in schema via `omitempty` and optional:"true".
			var tag reflect.StructTag
			if parameter.Schema != nil && parameter.Schema.Type().Kind() == reflect.Slice {
				tag = `json:",omitempty" optional:"true"`
			}
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type(), Tag: tag})
		case state.KindQuery, state.KindForm:

			if uniqueQuery[parameter.In.Name] {
				continue
			}
			uniqueQuery[parameter.In.Name] = true
			// Repeated (slice) params are optional regardless of "required" tag.
			// Otherwise, respect explicit required; default to optional.
			tag := reflect.StructTag(parameter.Tag)
			if parameter.Schema != nil && parameter.Schema.Type().Kind() == reflect.Slice {
				tag = `json:",omitempty" optional:"true"`
			} else if !strings.Contains(parameter.Tag, "required") {
				tag = `json:",omitempty"`
			}
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type(), Tag: tag})
		case state.KindRequestBody:
			// If body is a slice, mark optional in schema.
			var tag reflect.StructTag
			if parameter.Schema != nil && parameter.Schema.Type().Kind() == reflect.Slice {
				tag = `json:",omitempty" optional:"true"`
			}
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type(), Tag: tag})
		}
	}

	// Include selector (limit/offset/fields/page) for read components when available
	if components.View != nil && components.View.Selector != nil {
		if p := components.View.Selector.LimitParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] { // avoid duplicates
				uniqueQuery[p.In.Name] = true
				inputFields = append(inputFields, reflect.StructField{Name: strings.Title(p.Name), Type: p.Schema.Type(), Tag: `json:",omitempty"`})
			}
		}
		if p := components.View.Selector.OffsetParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				inputFields = append(inputFields, reflect.StructField{Name: strings.Title(p.Name), Type: p.Schema.Type(), Tag: `json:",omitempty"`})
			}
		}
		if p := components.View.Selector.FieldsParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				// Fields is a []string – ensure optional in schema
				inputFields = append(inputFields, reflect.StructField{Name: strings.Title(p.Name), Type: p.Schema.Type(), Tag: `json:",omitempty" optional:"true"`})
			}
		}
		if p := components.View.Selector.PageParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				inputFields = append(inputFields, reflect.StructField{Name: strings.Title(p.Name), Type: p.Schema.Type(), Tag: `json:",omitempty"`})
			}
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
	// Also expose view selector pagination controls in URI template if present
	if provider != nil {
		if comp, err := provider.Component(context.Background()); err == nil && comp.View != nil && comp.View.Selector != nil {
			if p := comp.View.Selector.LimitParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.OffsetParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.FieldsParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.PageParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
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
		result, rpcErr := r.handleMcpRead(ctx, &request.Params, &mcpResourceTemplate, aRoute, provider)
		if rpcErr != nil {
			return nil, rpcErr
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

func (r *Router) handleMcpRead(ctx context.Context, params *schema.ReadResourceRequestParams, template *schema.ResourceTemplate, aRoute *Route, provider *repository.Provider) ([]schema.ReadResourceResultContentsElem, *jsonrpc.Error) {
	URI := furl.Path(params.Uri)
	URL := fmt.Sprintf("http://localhost/%v", URI) // fallback to a local URL for now, this should be replaced with the actual service URL
	component, err := provider.Component(ctx)      // ensure the provider is initialized
	if err != nil {
		return nil, jsonrpc.NewInternalError(fmt.Errorf("failed to get component from provider: %w", err).Error(), nil)
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
	if responseWriter.Code == http.StatusUnauthorized {
		return nil, r.mcpUnauthorizedError()
	}
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
