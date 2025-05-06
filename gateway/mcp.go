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
	name := component.View.Ref
	if name == "" {
		name = component.View.Name
	}
	description := component.Description
	if description == "" {
		if aPath.Method == http.MethodGet {
			description = "Query data from " + name + " view; source: " + component.View.Source()
		} else {
			description = "Modify data in " + name + " view; destination: " + component.View.Table
		}
	}
	mcpTool := schema.Tool{
		Name:        strings.ReplaceAll(name, "#", ""),
		Description: &description,
		InputSchema: schema.ToolInputSchema{},
	}

	err = mcpTool.InputSchema.Load(reflect.New(toolInputType).Interface())
	if err != nil {
		return err
	}
	tool := &extension.Tool{
		Tool: mcpTool,
		Handler: func(ctx context.Context, params *schema.CallToolRequestParams) (*schema.CallToolResult, error) {
			URI := furl.Path(aPath.URI)
			URL := fmt.Sprintf("http://localhost/%v", URI) // fallback to a local URL for now, this should be replaced with the actual service URL
			values := url.Values{}
			var body io.Reader
			for _, parameter := range component.Input.Type.Parameters {
				paramName := strings.Title(parameter.Name)
				value := params.Arguments[paramName]
				switch parameter.In.Kind {
				case state.KindPath:
					URL = strings.ReplaceAll(URL, "{"+parameter.In.Name+"}", fmt.Sprintf("%s", value))
				case state.KindQuery, state.KindForm:
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
			httpRequest, _ := http.NewRequest(http.MethodGet, URL, body)
			if tokenValue := ctx.Value(authorization.TokenKey); tokenValue != nil {
				if token, ok := tokenValue.(*authorization.Token); ok {
					httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.Token))
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
		},
	}
	r.mcp.AddTool(tool)
	return nil
}

func (r *Router) buildToolInputType(components *repository.Component) reflect.Type {
	var inputFields []reflect.StructField
	for _, parameter := range components.Input.Type.Parameters {
		name := strings.Title(parameter.Name)
		switch parameter.In.Kind {
		case state.KindPath:
			inputFields = append(inputFields, reflect.StructField{Name: name, Type: parameter.Schema.Type()})
		case state.KindQuery, state.KindForm:
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

func (r *Router) buildResourceTemplatesIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
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
	shallUseTemplate := len(parameterNames) > 0 || strings.Contains(aPath.URI, "{")
	URL := furl.Join("datly://localhost", aPath.URI)
	if shallUseTemplate {

		if len(parameterNames) > 0 {
			// append query parameters to the URL
			URL += "{?" + strings.Join(parameterNames, ",") + "}"
		}
		name := aPath.URI + "." + aPath.View.Ref
		description := aPath.Description
		if description == "" {
			// fallback to view name if no description is provided
			description = aPath.View.Ref
		}
		mimeType := "application/json"
		mcpResourceTemplate := schema.ResourceTemplate{
			UriTemplate: URL,
			Name:        name,
			Description: &description,
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
	}
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
	httpRequest, _ := http.NewRequest(http.MethodGet, URL, nil)

	if tokenValue := ctx.Value(authorization.TokenKey); tokenValue != nil {
		if token, ok := tokenValue.(*authorization.Token); ok {
			httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.Token))
		}
	}

	aRoute.Handle(responseWriter, httpRequest) // route the request to the actual handler
	var result []schema.ReadResourceResultContentsElem
	mimeType := ""
	if template.MimeType != nil {
		mimeType = *template.MimeType
	}
	result = append(result, schema.ReadResourceResultContentsElem{
		Uri:      URL,                          // The URI of the resource
		MimeType: &mimeType,                    // The MIME type of the resource
		Blob:     responseWriter.Body.String(), // The actual data returned from the request
	})
	return result, nil

}
