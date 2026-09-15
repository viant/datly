package generate

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	xdocs "github.com/viant/xdatly/docs"
)

type Plan struct {
	OwnerIdentity    string
	ComponentPackage string
	Destinations     map[string]string
	Aliases          []TypeAlias
	Package          string
	GoPackage        string
	Holder           string
	ProjectRoot      string
	ShapePackages    []*Plan
	ShapesOnly       bool
	Documentation    xdocs.Source
	Static           *spec.StaticContent
	ComponentName    string
	Description      string
	Example          string
	Handler          string
	Routes           []RoutePlan
	Connector        string
	Report           *spec.ReportSettings
	Settings         dtag.Settings
	Imports          []spec.ImportSpec

	ViewDest     string
	RouterDest   string
	RootViewName string
	RootViewType string
	RootSource   string
	Views        []ViewPlan

	Input  ContractPlan
	Output ContractPlan

	HelperTypes     []HelperType
	GeneratedTypes  []GeneratedTypePlan
	GoHandler       *GoHandlerPlan
	ContractHandler *ContractHandlerPlan
	MutationHandler *MutationHandlerPlan
	FactoryLink     *FactoryLinkPlan
	HookScaffold    *HookScaffoldPlan
	VeltyHandler    *VeltyHandlerPlan
	Resources       *ResourcePlan
	EntitySupport   *EntitySupportPlan
}

// PackageName is the Go package name used by the scaffold emission owner.
func (p *Plan) PackageName() string {
	if p.GoPackage != "" {
		return p.GoPackage
	}
	return lowerSnake(p.ComponentName)
}
func (p *Plan) HolderName() string {
	if p.Holder != "" {
		return p.Holder
	}
	return "Component"
}

// RoutePlan is the route-specific metadata emitted on one typed component
// holder field. Handler identity remains component-wide on Plan.
type RoutePlan struct {
	Name         string
	Path         string
	Method       string
	Marshaller   string
	APIKeyHeader string
	APIKeyValue  string
	MCP          []*spec.MCPExposure
}

type ContractOwnership string

const (
	ContractGenerated ContractOwnership = "generated"
	ContractLinked    ContractOwnership = "linked"
)

// ContractPlan keeps one input/output contract's type, destination, fields,
// and ownership together so emission cannot mix linked and generated state.
type ContractPlan struct {
	Package       string
	Type          string
	Destination   string
	Fields        []Field
	Ownership     ContractOwnership
	DescriptorKey string
}

// Field returns a contract field by its generated Go name.
func (p *ContractPlan) Field(name string) (Field, bool) {
	if p == nil {
		return Field{}, false
	}
	for _, field := range p.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return Field{}, false
}

type Field struct {
	// ExplicitType authorizes only this field's exact planned CAST type during shape persistence.
	ExplicitType bool
	// RelationHolder carries canonical relation cardinality authority. Persistence
	// may change only a proven, unedited generated holder between *T and []*T.
	RelationHolder bool
	Name           string
	Type           string
	Tag            string
	Source         string
}

type HelperType struct {
	Package string
	Name    string
	Fields  []Field
}

// ViewPlan is one generated canonical view type. Relations reference another
// ViewPlan by its local type name rather than creating a parallel graph.
type ViewPlan struct {
	Package         string
	Identity        string
	Name            string
	Type            string
	Destination     string
	Fields          []Field
	SetMarkerFields []string
	Ownership       ViewOwnership
	DescriptorKey   string
}

// Field returns a generated view field by its Go name.
func (p *ViewPlan) Field(name string) (Field, bool) {
	if p == nil {
		return Field{}, false
	}
	for _, field := range p.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return Field{}, false
}

// ViewByType returns the final view plan that owns typeName.
func (p *Plan) ViewByType(typeName string) *ViewPlan {
	if p == nil {
		return nil
	}
	for index := range p.Views {
		if p.Views[index].Type == typeName {
			return &p.Views[index]
		}
	}
	return nil
}

type ViewOwnership string

const (
	ViewGenerated ViewOwnership = "generated"
	ViewLinked    ViewOwnership = "linked"
)

func (r *planResolver) resolveBase() (*Plan, error) {
	component := r.input.Component
	declarations := r.input.Declarations
	if component == nil {
		return nil, nil
	}
	name := strings.TrimSpace(component.Name)
	if name == "" {
		name = strings.TrimSpace(component.Key.Name)
	}
	snake := lowerSnake(name)
	settings := component.Settings
	var generation *spec.GenerationSettings
	if settings != nil {
		generation = settings.Generation
	}
	inheritedViewDest := ""
	if generation != nil {
		inheritedViewDest = strings.TrimSpace(generation.ViewFile)
	}
	materializeViewDestinations(component.RootView, inheritedViewDest, map[*spec.View]bool{})
	for _, view := range component.Views {
		materializeViewDestinations(view, inheritedViewDest, map[*spec.View]bool{})
	}
	rootViewType := upperCamel(name)
	if !strings.HasSuffix(rootViewType, "View") {
		rootViewType += "View"
	}
	if component.RootView != nil && strings.TrimSpace(component.RootView.TypeName) != "" {
		rootViewType = strings.TrimSpace(component.RootView.TypeName)
	}
	outputFields, err := resolveOutputFields(component, declarations, rootViewType)
	if err != nil {
		return nil, err
	}
	inputFields, err := resolveInputFields(component, declarations)
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		ComponentName: name,
		OwnerIdentity: component.Key.String(), ComponentPackage: r.input.TargetPackage,
		Package: r.input.TargetPackage, GoPackage: r.input.PackageName, ProjectRoot: r.input.ProjectRoot,
		Documentation: component.Documentation.Clone(),
		Description:   strings.TrimSpace(component.Description),
		Example:       strings.TrimSpace(component.Example),
		Connector:     r.rootConnector(),
		RootViewName:  component.RootView.CanonicalName(),
		RootViewType:  rootViewType,
		RootSource:    r.rootSource(),
		ViewDest:      snake + ".go",
		RouterDest:    snake + "_router.go",
		Input: ContractPlan{
			Type: upperCamel(name) + "Input", Destination: snake + "_input.go",
			Fields: inputFields, Ownership: ContractGenerated,
		},
		Output: ContractPlan{
			Type: upperCamel(name) + "Output", Destination: snake + "_output.go",
			Fields: outputFields, Ownership: ContractGenerated,
		},
		HelperTypes: resolveHelperTypes(component, declarations),
	}
	if r.input.PackageName != "" {
		plan.Holder = upperCamel(name) + "Component"
	}
	if component.TypeContext != nil {
		plan.Imports = append(plan.Imports, component.TypeContext.Imports...)
	}
	plan.Routes, plan.Handler, err = resolveRoutes(component.Routes)
	if err != nil {
		return nil, err
	}
	if settings == nil {
		if component.RootView != nil {
			if v := strings.TrimSpace(component.RootView.Dest); v != "" {
				plan.ViewDest = v
			}
		}
		return plan, nil
	}
	plan.Settings = dtag.SettingsFromSpec(settings)
	if plan.Connector == "" {
		plan.Connector = strings.TrimSpace(settings.DefaultConnector)
	}
	if settings.Report != nil {
		plan.Report = settings.Report.Clone()
	}
	if generation != nil {
		if v := strings.TrimSpace(generation.ViewFile); v != "" {
			plan.ViewDest = v
		}
		if v := strings.TrimSpace(generation.InputFile); v != "" {
			plan.Input.Destination = v
		}
		if v := strings.TrimSpace(generation.OutputFile); v != "" {
			plan.Output.Destination = v
		}
		if v := strings.TrimSpace(generation.RouterFile); v != "" {
			plan.RouterDest = v
		}
	}
	if v := strings.TrimSpace(settings.InputType); v != "" {
		plan.Input.Type = v
	}
	if v := strings.TrimSpace(settings.OutputType); v != "" {
		plan.Output.Type = v
	}
	if component.RootView != nil {
		if v := strings.TrimSpace(component.RootView.Dest); v != "" {
			plan.ViewDest = v
		}
	}
	return plan, nil
}

func resolveRoutes(routes []*spec.Route) ([]RoutePlan, string, error) {
	result := make([]RoutePlan, 0, len(routes))
	handler := ""
	seen := map[string]bool{}
	for index, route := range routes {
		if route == nil {
			return nil, "", fmt.Errorf("route %d is nil", index+1)
		}
		method := strings.ToUpper(strings.TrimSpace(route.Method))
		path := strings.TrimSpace(route.Path)
		if method == "" || path == "" {
			return nil, "", fmt.Errorf("route %d requires method and path", index+1)
		}
		identity := method + " " + path
		if seen[identity] {
			return nil, "", fmt.Errorf("route %q is declared more than once", identity)
		}
		seen[identity] = true
		if candidate := strings.TrimSpace(route.Handler); candidate != "" {
			if handler != "" && handler != candidate {
				return nil, "", fmt.Errorf("routes declare conflicting handlers %q and %q", handler, candidate)
			}
			handler = candidate
		}
		cloned := route.Clone()
		result = append(result, RoutePlan{
			Name: strings.TrimSpace(route.Name), Path: path, Method: method,
			Marshaller: strings.TrimSpace(route.Marshaller), APIKeyHeader: strings.TrimSpace(route.APIKeyHeader),
			APIKeyValue: route.APIKeyValue,
			MCP:         cloned.MCP,
		})
	}
	return result, handler, nil
}

func materializeViewDestinations(view *spec.View, inherited string, visited map[*spec.View]bool) {
	if view == nil || visited[view] {
		return
	}
	visited[view] = true
	effective := strings.TrimSpace(view.Dest)
	if effective == "" {
		effective = strings.TrimSpace(inherited)
		if effective != "" {
			view.Dest = effective
		}
	}
	for _, relation := range view.Relations {
		if relation != nil {
			materializeViewDestinations(relation.View, effective, visited)
		}
	}
}

func exportedName(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	runes := []rune(input)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func lowerSnake(input string) string {
	var b strings.Builder
	var prev rune
	lastUnderscore := false
	for i, r := range strings.TrimSpace(input) {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(prev) || unicode.IsDigit(prev)) && !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
		if unicode.IsUpper(r) {
			b.WriteRune(unicode.ToLower(r))
			lastUnderscore = false
			prev = r
			continue
		}
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
			lastUnderscore = false
		default:
			if b.Len() > 0 && !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
		prev = r
	}
	ret := strings.Trim(b.String(), "_")
	if ret == "" {
		return "component"
	}
	return ret
}

func upperCamel(input string) string {
	parts := strings.Split(lowerSnake(input), "_")
	if len(parts) == 0 {
		return "Component"
	}
	var b strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		runes[0] = unicode.ToUpper(runes[0])
		b.WriteString(string(runes))
	}
	ret := b.String()
	if ret == "" {
		return "Component"
	}
	return ret
}
