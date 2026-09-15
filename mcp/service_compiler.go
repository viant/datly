package mcp

import (
	"context"
	"fmt"
	documentation "github.com/viant/datly/documentation"
	"sort"
	"strings"

	"github.com/viant/datly/mcp/invocation"
	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	mcpserver "github.com/viant/mcp-protocol/server"
)

const DefaultResourceBaseURI = "datly://localhost"

type serviceCompiler struct {
	config Config
}

type compiledPlans struct {
	inputs    *registry.InputCatalog
	tools     map[string]*tool.Plan
	resources []*mcpresource.Plan
}

func (c *serviceCompiler) Compile(ctx context.Context) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	components, err := c.components()
	if err != nil {
		return nil, err
	}
	plans, err := c.plans(components)
	if err != nil {
		return nil, err
	}
	folders := append([]mcpresource.Folder(nil), c.config.Folders...)
	for _, entry := range components {
		if visibility, ok := c.config.Invoker.(interface{ ExposesComponent(spec.Key) bool }); ok && !visibility.ExposesComponent(entry.Component.Key) {
			continue
		}
		if entry.Component.Settings != nil {
			for _, folder := range entry.Component.Settings.MCPFolders {
				folders = append(folders, mcpresource.Folder{Namespace: folder.Namespace, Root: folder.Root, URIPrefix: folder.URIPrefix, Skills: folder.Skills})
			}
		}
	}
	files, err := (mcpresource.Publisher{Resources: c.config.Resources}).Compile(ctx, folders)
	if err != nil {
		return nil, err
	}
	plans.resources = append(plans.resources, files...)
	resources, err := mcpresource.NewCatalog(plans.resources)
	if err != nil {
		return nil, err
	}
	catalog := newCatalog(plans.tools, resources)
	var policy *authorization.Policy
	if len(c.config.Indexed) == 0 {
		policy, err = compileAuthorizationPolicy(c.config.Authorization, catalog)
	} else {
		policy, err = compileIndexedAuthorizationPolicy(c.config.Authorization, catalog, c.config.Indexed)
	}
	if err != nil {
		return nil, err
	}
	service, err := c.publish(catalog, policy)
	if err != nil {
		return nil, err
	}
	if len(c.config.Indexed) > 0 {
		service.lazy, err = newLazyCatalog(c.config, service)
		if err != nil {
			return nil, err
		}
	}
	return service, nil
}

func (c *serviceCompiler) components() ([]*registry.RegisteredComponent, error) {
	if c == nil || c.config.Invoker == nil {
		return nil, fmt.Errorf("MCP component invoker is required")
	}
	result := append([]*registry.RegisteredComponent(nil), c.config.Components...)
	for _, registered := range result {
		if registered == nil || registered.Component == nil || registered.Input == nil {
			return nil, fmt.Errorf("MCP registered component and input contract are required")
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Component.Key.String() < result[j].Component.Key.String()
	})
	return result, nil
}

func (c *serviceCompiler) plans(components []*registry.RegisteredComponent) (*compiledPlans, error) {
	resourceCompiler, err := mcpresource.NewCompiler(resourceBaseURI(c.config.ResourceBaseURI))
	if err != nil {
		return nil, err
	}
	inputs, err := registry.NewInputCatalog(components)
	if err != nil {
		return nil, err
	}
	result := &compiledPlans{inputs: inputs, tools: map[string]*tool.Plan{}}
	toolCompiler := tool.NewCompiler()
	for _, registered := range components {
		if visibility, ok := c.config.Invoker.(interface{ ExposesComponent(spec.Key) bool }); ok && !visibility.ExposesComponent(registered.Component.Key) {
			continue
		}
		for _, route := range registered.Component.Routes {
			if err := c.compileRoute(result, toolCompiler, resourceCompiler, registered, route); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (c *serviceCompiler) compileRoute(result *compiledPlans, toolCompiler *tool.Compiler, resourceCompiler *mcpresource.Compiler, registered *registry.RegisteredComponent, route *spec.Route) error {
	if err := validateExposedRoute(registered.Component, route); err != nil {
		return err
	}
	if route == nil || len(route.MCP) == 0 {
		return nil
	}
	ref := spec.RouteRef{Method: route.Method, Path: route.Path}
	contract, ok := registered.Input.ForRoute(ref)
	if !ok {
		return fmt.Errorf("MCP route %s has no exact input contract", ref.String())
	}
	for _, authored := range route.MCP {
		if authored == nil {
			return fmt.Errorf("MCP route %s has a nil exposure", contract.Route().String())
		}
		exposure, err := resolveExposure(registered.Component, route, authored, c.config.Resources)
		if err != nil {
			return err
		}
		authoredDescription := exposure.Description
		if authoredDescription == "" {
			authoredDescription = registered.Component.Description
		}
		exposure.Description = registered.Documentation.Operation(route.Path, documentation.Annotation{Description: authoredDescription}).Description
		switch exposure.Kind {
		case spec.MCPExposureTool:
			fields, err := result.inputs.FieldsFor(registered.Component.Key, ref)
			if err != nil {
				return err
			}
			plan, compileErr := toolCompiler.Compile(tool.Input{Fields: fields, Example: registered.Documentation.Operation(route.Path, documentation.Annotation{Example: registered.Component.Example}).Example, TransportReady: registered.Output != nil && registered.Output.TransportReady(), Documentation: registered.Documentation, Component: registered.Component.Key, Exposure: exposure, Contract: contract})
			if compileErr != nil {
				return compileErr
			}
			name := plan.Metadata().Name
			if _, exists := result.tools[name]; exists {
				return fmt.Errorf("duplicate MCP tool name %q", name)
			}
			result.tools[name] = plan
		case spec.MCPExposureResource, spec.MCPExposureResourceTemplate:
			plan, compileErr := resourceCompiler.Compile(mcpresource.Input{
				Component: registered.Component.Key, Route: route, Exposure: exposure, Contract: contract,
			})
			if compileErr != nil {
				return compileErr
			}
			result.resources = append(result.resources, plan)
		default:
			return fmt.Errorf("unknown MCP exposure kind %q", exposure.Kind)
		}
	}
	return nil
}

func (c *serviceCompiler) publish(catalog *Catalog, policy *authorization.Policy) (*Service, error) {
	protocolRegistry := mcpserver.NewRegistry()
	componentInvoker := invocation.New(invocation.Config{Invoker: c.config.Invoker, Client: c.config.Client})
	resourceHandler := mcpresource.NewHandler(catalog.resources, componentInvoker)
	for _, name := range catalog.names {
		plan, _ := catalog.Tool(name)
		handler := tool.NewHandler(plan, componentInvoker)
		protocolRegistry.RegisterTool(&mcpserver.ToolEntry{Metadata: plan.Metadata(), Handler: handler.Handle})
	}
	for _, metadata := range catalog.resources.Resources() {
		protocolRegistry.RegisterResource(metadata, resourceHandler.Handle)
	}
	for _, metadata := range catalog.resources.Templates() {
		protocolRegistry.RegisterResourceTemplate(metadata, resourceHandler.Handle)
	}
	if len(catalog.resources.Names()) > 0 {
		protocolRegistry.Methods.Put(schema.MethodResourcesRead, true)
	}
	if err := catalog.resources.RegisterSkills(protocolRegistry); err != nil {
		return nil, err
	}
	return &Service{catalog: catalog, registry: protocolRegistry, resources: resourceHandler, policy: policy}, nil
}

func resourceBaseURI(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return DefaultResourceBaseURI
}

func validateExposedRoute(component *spec.Component, route *spec.Route) error {
	if route == nil {
		return fmt.Errorf("MCP component %s has a nil route", component.Key.String())
	}
	if len(route.MCP) > 0 && (strings.TrimSpace(route.APIKeyHeader) != "" || route.APIKeyValue != "") {
		return fmt.Errorf("MCP route %s is guarded by an HTTP API key", (spec.RouteRef{Method: route.Method, Path: route.Path}).String())
	}
	return nil
}
