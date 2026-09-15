package transcribe

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type projectComponentOutput struct {
	descriptor *x.Type
	origin     typecatalog.TypeOrigin
}

type projectRouteTarget struct {
	component *projectComponentTarget
	ref       spec.RouteRef
}

type projectComponentTarget struct {
	identity  string
	source    string
	routes    []ProjectRoute
	prepared  *preparedProjectComponent
	output    projectComponentOutput
	hasOutput bool
}

type projectComponentResolver struct {
	components []preparedProjectComponent
	persisted  []ProjectComponent
}

func newProjectComponentResolver(components []preparedProjectComponent, persisted []ProjectComponent) *projectComponentResolver {
	return &projectComponentResolver{components: components, persisted: persisted}
}

func (r *projectComponentResolver) resolve() error {
	targets, outputs, err := r.targets()
	if err != nil {
		return err
	}
	if err := r.registerOutputs(outputs); err != nil {
		return err
	}
	routes, sources, err := r.index(targets)
	if err != nil {
		return err
	}
	for index := range r.components {
		owner := &r.components[index]
		for _, param := range spec.EffectiveParameters(owner.compiled.Component.Parameters) {
			if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), string(spec.KindComponent)) {
				continue
			}
			target, err := r.target(owner, param.Source.Name, routes, sources)
			if err != nil {
				return fmt.Errorf("component %q parameter %q: %w", owner.identity, param.Name, err)
			}
			if !target.component.hasOutput {
				return fmt.Errorf("component %q parameter %q: persisted component %q has no output type metadata; regenerate that dependency", owner.identity, param.Name, target.component.identity)
			}
			param.Source.Name = target.ref.String()
			if strings.TrimSpace(param.TypeExpr) == "" {
				output := target.component.output
				param.TypeExpr = "*" + output.descriptor.PkgPath + "." + output.descriptor.Name
			}
			if target.component.prepared != nil {
				if owner.dependencies == nil {
					owner.dependencies = map[string]bool{}
				}
				owner.dependencies[target.component.identity] = true
			}
		}
	}
	return nil
}

func (r *projectComponentResolver) targets() ([]projectComponentTarget, map[string]projectComponentOutput, error) {
	targets := make([]projectComponentTarget, 0, len(r.components)+len(r.persisted))
	outputs := make(map[string]projectComponentOutput, len(r.components)+len(r.persisted))
	incoming := make(map[string]bool, len(r.components))
	for index := range r.components {
		component := &r.components[index]
		output, err := component.output()
		if err != nil {
			return nil, nil, fmt.Errorf("resolve component %q output: %w", component.identity, err)
		}
		incoming[component.identity] = true
		outputs[component.identity] = output
		targets = append(targets, projectComponentTarget{
			identity: component.identity, source: component.compiled.Source.Path,
			routes: component.routes(), prepared: component,
			output: output, hasOutput: true,
		})
	}
	for _, component := range r.persisted {
		identity := component.Key.String()
		if incoming[identity] {
			continue
		}
		output, ok, err := component.output()
		if err != nil {
			return nil, nil, fmt.Errorf("resolve persisted component %q output: %w", identity, err)
		}
		target := projectComponentTarget{
			identity: identity, source: component.Source, routes: append([]ProjectRoute(nil), component.Routes...),
			output: output, hasOutput: ok,
		}
		if ok {
			outputs[identity] = output
		}
		targets = append(targets, target)
	}
	return targets, outputs, nil
}

func (c ProjectComponent) output() (projectComponentOutput, bool, error) {
	key := strings.TrimSpace(c.OutputType)
	if key == "" {
		return projectComponentOutput{}, false, nil
	}
	packagePath, name, err := (xshape.Resolver{}).CanonicalReference(key)
	if err != nil {
		return projectComponentOutput{}, false, fmt.Errorf("output type %q is not a package-qualified named type", key)
	}
	reference, err := (xshape.Resolver{}).Reference(name)
	if err != nil || len(reference.Wrappers) != 0 || reference.Name != reference.BaseName || !reference.Exported() {
		return projectComponentOutput{}, false, fmt.Errorf("output type %q is not an exported named type", key)
	}
	origin := c.OutputOrigin
	if origin != typecatalog.TypeOriginPackage && origin != typecatalog.TypeOriginGenerated {
		return projectComponentOutput{}, false, fmt.Errorf("output type %q has unsupported origin %q", key, c.OutputOrigin)
	}
	return projectComponentOutput{
		descriptor: &x.Type{PkgPath: packagePath, Name: name},
		origin:     origin,
	}, true, nil
}

func (c *preparedProjectComponent) output() (projectComponentOutput, error) {
	if c == nil || c.compiled == nil || c.plan == nil {
		return projectComponentOutput{}, fmt.Errorf("prepared component is required")
	}
	if c.plan.Output.Ownership == gen.ContractLinked {
		if c.compiled.TypeResolver == nil || strings.TrimSpace(c.plan.Output.DescriptorKey) == "" {
			return projectComponentOutput{}, fmt.Errorf("linked output requires type authority")
		}
		descriptor, err := c.compiled.TypeResolver.Descriptor(c.plan.Output.DescriptorKey)
		if err != nil {
			return projectComponentOutput{}, err
		}
		if descriptor == nil || strings.TrimSpace(descriptor.PkgPath) == "" || strings.TrimSpace(descriptor.Name) == "" {
			return projectComponentOutput{}, fmt.Errorf("linked output is not a named package type")
		}
		return projectComponentOutput{descriptor: descriptor, origin: typecatalog.TypeOriginPackage}, nil
	}
	name := strings.TrimSpace(c.plan.Output.Type)
	if name == "" || strings.ContainsAny(name, "*[]./") {
		return projectComponentOutput{}, fmt.Errorf("generated output type %q is not a local named type", name)
	}
	return projectComponentOutput{
		descriptor: &x.Type{PkgPath: func() string {
			if c.plan.Output.Package != "" {
				return c.plan.Output.Package
			}
			return c.targetPackage
		}(), Name: name},
		origin: typecatalog.TypeOriginGenerated,
	}, nil
}

func (r *projectComponentResolver) registerOutputs(outputs map[string]projectComponentOutput) error {
	identities := make([]string, 0, len(outputs))
	for identity := range outputs {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	for index := range r.components {
		compiled := r.components[index].compiled
		catalog := compiled.Source.Types
		for _, identity := range identities {
			output := outputs[identity]
			key := output.descriptor.Key()
			if _, ok, err := catalog.Resolve(compiled.TypeAuthority, key); err != nil {
				return err
			} else if ok {
				continue
			}
			if err := catalog.Register(output.origin, output.descriptor); err != nil {
				return fmt.Errorf("register component output %q: %w", key, err)
			}
		}
		resolver, err := typecatalog.NewResolver(catalog, compiled.TypeAuthority, compiled.TypeContext)
		if err != nil {
			return err
		}
		compiled.TypeResolver = resolver
	}
	return nil
}

func (r *projectComponentResolver) index(components []projectComponentTarget) (map[string]*projectRouteTarget, map[string][]*projectRouteTarget, error) {
	routes := map[string]*projectRouteTarget{}
	sources := map[string][]*projectRouteTarget{}
	for index := range components {
		component := &components[index]
		sourceKey := r.sourceIdentity(component.source)
		for _, route := range component.routes {
			ref, err := spec.ParseRouteRef(spec.RouteRef{Method: route.Method, Path: route.Path}.String())
			if err != nil {
				return nil, nil, fmt.Errorf("component %q has an incomplete route", component.identity)
			}
			key := ref.String()
			if existing := routes[key]; existing != nil && existing.component.identity != component.identity {
				return nil, nil, fmt.Errorf("route %q is shared by components %q and %q", ref.Method+" "+ref.Path, existing.component.identity, component.identity)
			}
			target := &projectRouteTarget{component: component, ref: ref}
			routes[key] = target
			if sourceKey != "" {
				sources[sourceKey] = append(sources[sourceKey], target)
			}
		}
	}
	return routes, sources, nil
}

func (r *projectComponentResolver) target(owner *preparedProjectComponent, value string, routes map[string]*projectRouteTarget, sources map[string][]*projectRouteTarget) (*projectRouteTarget, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("component source is required")
	}
	if ref, err := spec.ParseRouteRef(value); err == nil {
		if target := routes[ref.String()]; target != nil {
			return target, nil
		}
		return nil, fmt.Errorf("component route %q was not found", ref.String())
	}
	method, location := r.splitReference(value)
	var candidates []*projectRouteTarget
	if strings.HasPrefix(location, "/") {
		for _, target := range routes {
			if target.ref.Path == location {
				candidates = append(candidates, target)
			}
		}
	} else {
		if owner == nil || owner.compiled == nil || owner.compiled.Source == nil {
			return nil, fmt.Errorf("relative component reference %q requires source context", value)
		}
		base := filepath.Dir(owner.compiled.Source.Path)
		candidates = append(candidates, sources[r.sourceIdentity(filepath.Join(base, location))]...)
	}
	if method != "" {
		filtered := candidates[:0]
		for _, candidate := range candidates {
			if strings.EqualFold(candidate.ref.Method, method) {
				filtered = append(filtered, candidate)
			}
		}
		candidates = filtered
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ref.String() < candidates[j].ref.String() })
	switch len(candidates) {
	case 0:
		return nil, fmt.Errorf("component reference %q was not found", value)
	case 1:
		return candidates[0], nil
	default:
		keys := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			keys = append(keys, candidate.ref.String())
		}
		return nil, fmt.Errorf("component reference %q is ambiguous: %s", value, strings.Join(keys, ", "))
	}
}

func (r *projectComponentResolver) splitReference(value string) (string, string) {
	if separator := strings.IndexByte(value, ':'); separator > 0 {
		method := strings.TrimSpace(value[:separator])
		location := strings.TrimSpace(value[separator+1:])
		if method != "" && location != "" && !strings.ContainsAny(method, `/\\`) {
			return strings.ToUpper(method), location
		}
	}
	return "", strings.TrimSpace(value)
}

func (r *projectComponentResolver) sourceIdentity(value string) string {
	value = filepath.Clean(strings.TrimSpace(value))
	if value == "" || value == "." {
		return ""
	}
	return strings.TrimSuffix(value, filepath.Ext(value))
}

func (r *projectComponentResolver) order() ([]preparedProjectComponent, error) {
	byIdentity := make(map[string]*preparedProjectComponent, len(r.components))
	for index := range r.components {
		byIdentity[r.components[index].identity] = &r.components[index]
	}
	state := map[string]uint8{}
	stack := []string{}
	result := make([]preparedProjectComponent, 0, len(r.components))
	var visit func(string) error
	visit = func(identity string) error {
		switch state[identity] {
		case 1:
			cycle := append(append([]string(nil), stack...), identity)
			return fmt.Errorf("component dependency cycle: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}
		component := byIdentity[identity]
		if component == nil {
			return fmt.Errorf("component dependency %q was not prepared", identity)
		}
		state[identity] = 1
		stack = append(stack, identity)
		dependencies := make([]string, 0, len(component.dependencies))
		for dependency := range component.dependencies {
			dependencies = append(dependencies, dependency)
		}
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		state[identity] = 2
		result = append(result, *component)
		return nil
	}
	for index := range r.components {
		if err := visit(r.components[index].identity); err != nil {
			return nil, err
		}
	}
	return result, nil
}
