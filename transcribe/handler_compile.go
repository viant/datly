package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func (c *Compiler) compileHandler(source *Source, header *dql.HandlerHeader, body string) (*Result, error) {
	if source.PackageComponent != nil || source.GoHandler != nil || source.VeltyHandler != nil {
		return nil, fmt.Errorf("legacy handler cannot overlay another component or handler asset")
	}
	var binding *HandlerBinding
	for _, candidate := range source.HandlerBindings {
		if candidate == nil || candidate.mapping.LegacyType != header.Type {
			continue
		}
		if binding != nil {
			return nil, fmt.Errorf("multiple handler mappings for %q", header.Type)
		}
		binding = candidate
	}
	if binding == nil || binding.input == nil || binding.output == nil {
		return nil, fmt.Errorf("legacy handler %q requires an explicit v1 factory mapping", header.Type)
	}
	mapping := binding.mapping
	if header.InputType != mapping.LegacyInput || header.OutputType != mapping.LegacyOutput {
		return nil, fmt.Errorf("legacy handler contract declarations conflict with mapping for %q", header.Type)
	}
	prepared := dql.PrepareSource(body)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	directives := prepared.Directives
	if strings.TrimSpace(prepared.SQL) != "" || len(directives.Views) != 0 || directives.Static != nil {
		return nil, fmt.Errorf("handler-only DQL cannot contain SQL or reader views")
	}
	if directives.Route != nil || directives.MCP != nil || directives.MCPOnly || directives.Internal || !directives.Settings.IsZero() || !directives.Documentation.IsZero() {
		return nil, fmt.Errorf("handler-only DQL settings conflict with the legacy header; retain settings in one declaration")
	}
	if prepared.TypeContext != nil && prepared.TypeContext.PackagePath != "" && prepared.TypeContext.PackagePath != mapping.DestinationPackage {
		return nil, fmt.Errorf("handler destination conflicts with #package")
	}
	connector := header.Connector
	if source.Connector != "" {
		if connector != "" && connector != source.Connector {
			return nil, fmt.Errorf("handler connector declarations conflict")
		}
		connector = source.Connector
	}
	typeContext := &spec.TypeContext{PackagePath: mapping.DestinationPackage}
	if prepared.TypeContext != nil {
		typeContext.Imports = append(typeContext.Imports, prepared.TypeContext.Imports...)
	}
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: source.Scope, Name: header.Name}, Name: header.Name, Description: header.Description,
		TypeContext: typeContext, Settings: &spec.Settings{DefaultConnector: connector},
		Routes: []*spec.Route{{Name: header.Name, Path: header.URI, Method: header.Method, Internal: header.Internal, Handler: mapping.FactoryPackage + "." + mapping.FactoryName}},
	}
	if header.MCPTool {
		component.Routes[0].MCP = []*spec.MCPExposure{{Kind: "tool", Name: header.Name}}
	}
	copy := *source
	var err error
	if source.Types == nil {
		copy.Types = typecatalog.NewCatalog()
	} else if copy.Types, err = source.Types.Clone(); err != nil {
		return nil, err
	}
	input, output := x.NewType(binding.input), x.NewType(binding.output)
	if err = copy.Types.LinkRuntimeAll(typecatalog.TypeOriginPackage, input, output); err != nil {
		return nil, err
	}
	context := compileTypeContext(&copy, typeContext)
	resolver, err := typecatalog.NewResolver(copy.Types, typecatalog.TranscribeAuthority, context)
	if err != nil {
		return nil, err
	}
	component, err = (bootstrap.ContractResolver{Component: component, InputType: input, OutputType: output, Types: resolver}).Resolve()
	if err != nil {
		return nil, err
	}
	if err = validateHandlerParameters(binding, directives.Params, component.Parameters, resolver); err != nil {
		return nil, err
	}
	result := &Result{
		Source: &copy, Component: component, TypeContext: context, TypeResolver: resolver, TypeAuthority: typecatalog.TranscribeAuthority,
		Contracts: gen.ContractReferences{
			Input:  &gen.ContractReference{Expression: input.Name, DescriptorKey: input.Key()},
			Output: &gen.ContractReference{Expression: output.Name, DescriptorKey: output.Key()},
		},
		ExternalHandler: &gen.ExternalHandler{Package: mapping.FactoryPackage, Name: mapping.FactoryName},
	}
	for _, parameter := range directives.Params {
		if reason, ok := mapping.UnusedParameters[parameter.Name]; ok {
			result.Diagnostics = append(result.Diagnostics, &Diagnostic{Code: "DQL-HANDLER-UNUSED", Severity: SeverityWarning, Path: source.Path,
				Message: fmt.Sprintf("legacy handler parameter %s is explicitly unused: %s", parameter.Name, reason)})
		}
	}
	return result, nil
}

func validateHandlerParameters(binding *HandlerBinding, authored, bound []*spec.Parameter, resolver *typecatalog.Resolver) error {
	fields, err := tag.NewBindingIndex(binding.input)
	if err != nil {
		return err
	}
	byName := map[string]*spec.Parameter{}
	for _, param := range bound {
		byName[param.Name] = param
	}
	unused := map[string]bool{}
	seen := map[string]bool{}
	for _, param := range authored {
		if seen[param.Name] {
			return fmt.Errorf("duplicate handler parameter %q", param.Name)
		}
		seen[param.Name] = true
		actual := byName[param.Name]
		if _, ignored := binding.mapping.UnusedParameters[param.Name]; ignored {
			if param.Required == nil || *param.Required || param.Source.Kind == "output" {
				return fmt.Errorf("only explicitly optional input declarations may be acknowledged as unused: %q", param.Name)
			}
			if actual != nil {
				return fmt.Errorf("unused handler parameter %q is bound by the input contract", param.Name)
			}
			unused[param.Name] = true
			continue
		}
		if actual == nil {
			return fmt.Errorf("handler parameter %q is not in the linked contract; explicitly acknowledge obsolete declarations", param.Name)
		}
		if param.Source != actual.Source {
			return fmt.Errorf("handler parameter %q binding conflicts with linked contract", param.Name)
		}
		if param.TypeExpr != "" && param.TypeExpr != "?" {
			typ, err := (xshape.Runtime{Lookup: resolver.Type}).Type(param.TypeExpr)
			if err != nil {
				return fmt.Errorf("handler parameter %q: %w", param.Name, err)
			}
			field, found, err := fields.Resolve(actual)
			if err != nil {
				return err
			}
			if !found || typ != field.Type {
				return fmt.Errorf("handler parameter %q type conflicts with linked contract", param.Name)
			}
		}
		if param.Required != nil && *param.Required != (actual.Required != nil && *actual.Required) {
			return fmt.Errorf("handler parameter %q requiredness conflicts with linked contract", param.Name)
		}
		// Contracts remain handwritten. Do not pretend newly authored predicates,
		// defaults, codecs or tags have been applied to their compiled Go types.
		remainder := *param
		remainder.Name, remainder.TypeExpr, remainder.Raw = "", "", ""
		remainder.Source, remainder.Required, remainder.Declaration = spec.BindSource{}, nil, ""
		if !reflect.DeepEqual(remainder, spec.Parameter{}) {
			return fmt.Errorf("handler parameter %q has unsupported contract overrides", param.Name)
		}
	}
	for name := range binding.mapping.UnusedParameters {
		if !unused[name] {
			return fmt.Errorf("unused handler parameter acknowledgment %q has no matching declaration", name)
		}
	}
	return nil
}
