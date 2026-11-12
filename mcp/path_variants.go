package mcp

import (
	"fmt"
	"strings"

	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func (c *serviceCompiler) compilePathVariants(plans *compiledPlans, compiler *tool.Compiler, entry *registry.RegisteredComponent) error {
	for _, base := range entry.Component.Routes {
		if base == nil || !strings.EqualFold(base.Method, "GET") {
			continue
		}
		for _, exposure := range base.MCP {
			if exposure == nil || exposure.Kind != spec.MCPExposureTool {
				continue
			}
			parent := plans.tools[exposure.Name]
			if parent == nil {
				continue
			}
			seen := map[string]bool{}
			for _, param := range spec.EffectiveParameters(entry.Component.Parameters) {
				if param == nil || param.Activation == nil || param.PathMCP == nil || *param.PathMCP || param.Activation.Matches(base.Path) {
					continue
				}
				path := param.Activation.URI
				if strings.HasPrefix(path, "/{") {
					path = strings.TrimRight(base.Path, "/") + path
				}
				// The authored activation and exact component-owned route contract
				// establish ownership; an absolute alternate need not share a URL prefix.
				ref := spec.RouteRef{Method: base.Method, Path: path}
				if seen[ref.String()] {
					continue
				}
				seen[ref.String()] = true
				contract, ok := entry.Input.ForRoute(ref)
				if !ok {
					return fmt.Errorf("MCP path variant %s has no input contract", ref.String())
				}
				fields, err := plans.inputs.FieldsFor(entry.Component.Key, ref)
				if err != nil {
					return err
				}
				variantExposure := exposure.Clone()
				variantExposure.Name = parent.Metadata().Name
				variant, err := compiler.Compile(tool.Input{Component: entry.Component.Key, Exposure: variantExposure, Contract: contract, Fields: fields, Output: entry.Output, OutputType: entry.OutputType, Documentation: entry.Documentation})
				if err != nil {
					return err
				}
				if err = parent.AddPathVariant(variant); err != nil {
					return fmt.Errorf("MCP tool %s: %w", exposure.Name, err)
				}
			}
		}
	}
	return nil
}
