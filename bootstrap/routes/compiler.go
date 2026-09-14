package routes

import (
	"fmt"
	route "github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"strings"
)

// Compiler materializes authored WithURI alternatives before input plans
// and MCP contracts are compiled. Runtime consumes only the resulting routes.
type Compiler struct{ Component *spec.Component }

func (e Compiler) Compile() error {
	if e.Component == nil {
		return fmt.Errorf("route component is required")
	}
	var bases []*spec.Route
	for _, candidate := range e.Component.Routes {
		if candidate == nil {
			continue
		}
		alternate := false
		for _, parameter := range e.Component.Parameters {
			if parameter != nil && parameter.IsTransportInput() && parameter.Activation != nil && parameter.Activation.Matches(candidate.Path) {
				alternate = true
				break
			}
		}
		if !alternate {
			bases = append(bases, candidate)
		}
	}
	known := map[string]*spec.Route{}
	for _, item := range e.Component.Routes {
		if item != nil {
			known[(spec.RouteRef{Method: item.Method, Path: item.Path}).String()] = item
		}
	}
	for _, parameter := range spec.EffectiveParameters(e.Component.Parameters) {
		if parameter == nil || parameter.Activation == nil || !parameter.IsTransportInput() {
			continue
		}
		uri := strings.TrimSpace(parameter.Activation.URI)
		if uri == "" {
			continue
		}
		for _, base := range bases {
			if base == nil {
				continue
			}
			if len(bases) > 1 && !strings.HasPrefix(uri, "/{") && !strings.HasPrefix(uri, strings.TrimRight(base.Path, "/")+"/") {
				continue
			}
			path := uri
			if strings.HasPrefix(uri, "/{") {
				path = strings.TrimRight(base.Path, "/") + uri
			}
			if _, err := route.CompilePathTemplate(path); err != nil {
				return fmt.Errorf("WithURI %s: %w", parameter.Name, err)
			}
			if path == base.Path {
				continue
			}
			key := (spec.RouteRef{Method: base.Method, Path: path}).String()
			if known[key] != nil {
				continue
			}
			alternate := *base
			alternate.Path = path
			alternate.MCP = nil
			suffix, err := e.Suffix(base.Path, path)
			if err != nil {
				return err
			}
			for _, exposure := range base.MCP {
				if exposure == nil {
					continue
				}
				copy := exposure.Clone()
				if copy.Kind == spec.MCPExposureTool {
					if parameter.PathMCP != nil && !*parameter.PathMCP {
						continue
					}
					name := strings.TrimSpace(copy.Name)
					if name == "" {
						name = strings.TrimSpace(base.Name)
					}
					if name == "" {
						name = e.Component.Name
					}
					copy.Name = name + suffix
				}
				alternate.MCP = append(alternate.MCP, copy)
			}
			e.Component.Routes = append(e.Component.Routes, &alternate)
			known[key] = &alternate
		}
	}
	// Base visibility is applied after alternatives copy their independent exposure.
	for _, parameter := range e.Component.Parameters {
		if parameter == nil || parameter.Activation == nil || !parameter.IsTransportInput() || parameter.MCP == nil || *parameter.MCP {
			continue
		}
		for _, base := range bases {
			if base == nil {
				continue
			}
			uri := strings.TrimSpace(parameter.Activation.URI)
			if len(bases) > 1 && !strings.HasPrefix(uri, "/{") && !strings.HasPrefix(uri, strings.TrimRight(base.Path, "/")+"/") {
				continue
			}
			var kept []*spec.MCPExposure
			for _, exposure := range base.MCP {
				if exposure != nil && exposure.Kind != spec.MCPExposureTool {
					kept = append(kept, exposure)
				}
			}
			base.MCP = kept
		}
	}
	for _, parameter := range e.Component.Parameters {
		if parameter == nil || parameter.Activation == nil || !parameter.IsTransportInput() || parameter.PathMCP == nil || *parameter.PathMCP {
			continue
		}
		for _, item := range e.Component.Routes {
			if item == nil || !parameter.Activation.Matches(item.Path) {
				continue
			}
			var kept []*spec.MCPExposure
			for _, exposure := range item.MCP {
				if exposure != nil && exposure.Kind != spec.MCPExposureTool {
					kept = append(kept, exposure)
				}
			}
			item.MCP = kept
		}
	}
	names := map[string]string{}
	for _, item := range e.Component.Routes {
		if item == nil {
			continue
		}
		for _, exposure := range item.MCP {
			if exposure == nil || exposure.Kind != spec.MCPExposureTool || exposure.Name == "" {
				continue
			}
			identity := (spec.RouteRef{Method: item.Method, Path: item.Path}).String()
			if previous, ok := names[exposure.Name]; ok && previous != identity {
				return fmt.Errorf("MCP tool %q collides between %s and %s", exposure.Name, previous, identity)
			}
			names[exposure.Name] = identity
		}
	}
	return nil
}

func (e Compiler) Suffix(basePath, alternatePath string) (string, error) {
	base, err := route.CompilePathTemplate(basePath)
	if err != nil {
		return "", err
	}
	alternate, err := route.CompilePathTemplate(alternatePath)
	if err != nil {
		return "", err
	}
	known := map[string]bool{}
	for _, name := range base.Parameters() {
		known[name] = true
	}
	var tokens []string
	for _, name := range alternate.Parameters() {
		if !known[name] {
			tokens = append(tokens, typecatalog.ExportedFieldName(name))
		}
	}
	if len(tokens) == 0 {
		left, right := strings.Split(strings.Trim(basePath, "/"), "/"), strings.Split(strings.Trim(alternatePath, "/"), "/")
		common := 0
		for common < len(left) && common < len(right) && left[common] == right[common] {
			common++
		}
		for _, item := range right[common:] {
			if !strings.HasPrefix(item, "{") {
				tokens = append(tokens, typecatalog.ExportedFieldName(item))
			}
		}
	}
	if len(tokens) == 0 {
		tokens = []string{"Route"}
	}
	return "By" + strings.Join(tokens, "And"), nil
}
