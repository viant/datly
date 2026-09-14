package resource

import (
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"

	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/runtime/registry"
	runtimeRoute "github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type Input struct {
	Component spec.Key
	Route     *spec.Route
	Exposure  *spec.MCPExposure
	Contract  *registry.RouteInputContract
}

type Compiler struct {
	scheme    string
	authority string
	basePath  string
}

func NewCompiler(baseURI string) (*Compiler, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURI))
	if err != nil {
		return nil, fmt.Errorf("parse MCP resource base URI: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("MCP resource base URI requires scheme and authority without user info, query, or fragment")
	}
	baseTemplate, err := runtimeRoute.CompilePathTemplate(defaultPath(parsed.Path))
	if err != nil {
		return nil, fmt.Errorf("MCP resource base URI path: %w", err)
	}
	if len(baseTemplate.Parameters()) != 0 {
		return nil, fmt.Errorf("MCP resource base URI must not contain placeholders")
	}
	return &Compiler{scheme: parsed.Scheme, authority: parsed.Host, basePath: strings.TrimSuffix(parsed.Path, "/")}, nil
}

func (c *Compiler) Compile(input Input) (*Plan, error) {
	if c == nil {
		return nil, fmt.Errorf("MCP resource compiler is required")
	}
	if input.Route == nil || input.Exposure == nil || input.Contract == nil {
		return nil, fmt.Errorf("MCP resource route, exposure, and exact input contract are required")
	}
	if input.Component.Kind != spec.KindComponent || strings.TrimSpace(input.Component.Name) == "" {
		return nil, fmt.Errorf("MCP resource requires a component key")
	}
	routeRef := spec.RouteRef{Method: input.Route.Method, Path: input.Route.Path}
	if routeRef.String() == "" || routeRef.String() != input.Contract.Route().String() {
		return nil, fmt.Errorf("MCP resource route %s does not match exact input contract %s", routeRef.String(), input.Contract.Route().String())
	}
	if strings.ToUpper(strings.TrimSpace(input.Route.Method)) != http.MethodGet {
		return nil, fmt.Errorf("MCP resource route %s must use GET", input.Contract.Route().String())
	}
	if input.Exposure.Kind != spec.MCPExposureResource && input.Exposure.Kind != spec.MCPExposureResourceTemplate {
		return nil, fmt.Errorf("MCP exposure kind %q is not a resource", input.Exposure.Kind)
	}
	name := strings.TrimSpace(input.Exposure.Name)
	if name == "" {
		return nil, fmt.Errorf("MCP resource name is required")
	}
	fullPath := joinPath(c.basePath, input.Route.Path)
	pathTemplate, err := runtimeRoute.CompilePathTemplate(fullPath)
	if err != nil {
		return nil, fmt.Errorf("MCP resource %q path: %w", name, err)
	}
	arguments, queryNames, err := resourceArguments(input.Contract, pathTemplate.Parameters())
	if err != nil {
		return nil, fmt.Errorf("MCP resource %q: %w", name, err)
	}
	binding, err := mcpinput.NewCompiler().Compile(arguments)
	if err != nil {
		return nil, fmt.Errorf("MCP resource %q binding: %w", name, err)
	}
	mimeType := strings.TrimSpace(input.Exposure.MIMEType)
	if mimeType == "" {
		mimeType = "application/json"
	}
	if _, _, err := mime.ParseMediaType(mimeType); err != nil {
		return nil, fmt.Errorf("MCP resource %q MIME type: %w", name, err)
	}
	description := strings.TrimSpace(input.Exposure.Description)
	publishedURI := c.scheme + "://" + c.authority + pathTemplate.EscapedTemplatePath()
	result := &Plan{
		kind: input.Exposure.Kind, target: exec.ComponentTarget{Component: input.Component, Route: input.Contract.Route()},
		input: input.Contract, path: pathTemplate, binding: binding, scheme: c.scheme, authority: c.authority,
	}
	switch input.Exposure.Kind {
	case spec.MCPExposureResource:
		if len(arguments) != 0 || len(pathTemplate.Parameters()) != 0 {
			return nil, fmt.Errorf("static resource cannot require path or query values")
		}
		result.resource = &schema.Resource{Name: name, Uri: publishedURI, MimeType: stringPointer(mimeType), Description: stringPointer(description)}
	case spec.MCPExposureResourceTemplate:
		if len(arguments) == 0 {
			return nil, fmt.Errorf("resource template requires at least one path or query value")
		}
		if len(queryNames) > 0 {
			publishedURI += "{?" + strings.Join(queryNames, ",") + "}"
		}
		result.template = &schema.ResourceTemplate{Name: name, UriTemplate: publishedURI, MimeType: stringPointer(mimeType), Description: stringPointer(description)}
	}
	return result, nil
}

func resourceArguments(contract *registry.RouteInputContract, placeholders []string) ([]mcpinput.Argument, []string, error) {
	pathFields := map[string]registry.InputField{}
	queryFields := map[string]registry.InputField{}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		kind := strings.ToLower(strings.TrimSpace(binding.Location.Kind))
		if kind != "path" && kind != "query" {
			if binding.Required != nil && *binding.Required {
				return nil, nil, fmt.Errorf("required %s field %q cannot be supplied by a resource URI", kind, field.Path())
			}
			continue
		}
		name := strings.TrimSpace(binding.Location.In)
		if name == "" {
			return nil, nil, fmt.Errorf("%s field %q has no source name", kind, field.Path())
		}
		target := queryFields
		if kind == "path" {
			target = pathFields
		}
		if _, ok := target[name]; ok {
			return nil, nil, fmt.Errorf("duplicate %s source %q", kind, name)
		}
		target[name] = field
	}
	placeholderSet := make(map[string]bool, len(placeholders))
	for _, name := range placeholders {
		placeholderSet[name] = true
		if _, ok := pathFields[name]; !ok {
			return nil, nil, fmt.Errorf("path placeholder %q has no exact route binding", name)
		}
	}
	for name := range pathFields {
		if !placeholderSet[name] {
			return nil, nil, fmt.Errorf("path binding %q has no route placeholder", name)
		}
	}
	queryNames := make([]string, 0, len(queryFields))
	for name := range queryFields {
		if placeholderSet[name] {
			return nil, nil, fmt.Errorf("source %q is declared by both path and query", name)
		}
		queryNames = append(queryNames, name)
	}
	sort.Strings(queryNames)
	arguments := make([]mcpinput.Argument, 0, len(placeholders)+len(queryNames))
	for _, name := range placeholders {
		field := pathFields[name]
		arguments = append(arguments, mcpinput.Argument{PublicName: name, Source: bindstate.Location{Kind: "path", In: name}, SourceType: field.SourceType()})
	}
	for _, name := range queryNames {
		field := queryFields[name]
		arguments = append(arguments, mcpinput.Argument{PublicName: name, Source: bindstate.Location{Kind: "query", In: name}, SourceType: field.SourceType()})
	}
	return arguments, queryNames, nil
}

func defaultPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func joinPath(basePath, routePath string) string {
	return strings.TrimSuffix(defaultPath(basePath), "/") + "/" + strings.TrimPrefix(routePath, "/")
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	result := value
	return &result
}
