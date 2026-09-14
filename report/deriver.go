package report

import (
	"fmt"
	"sort"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// reportDeriver owns one project derivation's component and route authority.
// Its indexes make collision checks and reservations one ordered operation.
type reportDeriver struct {
	compiler *ProjectCompiler
	keys     map[string]bool
	routes   map[string]string
}

func newReportDeriver(compiler *ProjectCompiler) *reportDeriver {
	return &reportDeriver{compiler: compiler, keys: map[string]bool{}, routes: map[string]string{}}
}

func (d *reportDeriver) compile(sources []Source) (*Project, error) {
	ordered := append([]Source(nil), sources...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].identity() < ordered[j].identity()
	})
	result := &Project{}
	for _, source := range ordered {
		if err := d.index(source); err != nil {
			return nil, err
		}
		result.components = append(result.components, source.Component.Clone())
	}
	var generated []*x.Type
	for _, source := range ordered {
		derived, descriptors, err := d.derive(source)
		if err != nil {
			return nil, err
		}
		for _, item := range derived {
			result.derived = append(result.derived, item)
			result.components = append(result.components, item.Component.Clone())
		}
		generated = append(generated, descriptors...)
	}
	if len(generated) > 0 {
		if d.compiler.config.Types == nil {
			return nil, fmt.Errorf("generated report inputs require a type catalog")
		}
		if err := d.compiler.config.Types.RegisterAll(typecatalog.TypeOriginGenerated, generated...); err != nil {
			return nil, fmt.Errorf("register generated report input types: %w", err)
		}
	}
	return result, nil
}

func (d *reportDeriver) derive(source Source) ([]*Derived, []*x.Type, error) {
	component := source.Component
	eligible := source.eligibleRoutes()
	if len(eligible) == 0 {
		return nil, nil, nil
	}
	if source.Input == nil || source.OutputType == nil {
		return nil, nil, fmt.Errorf("report source %s requires compiled input and output contracts", component.Key.String())
	}
	var result []*Derived
	var descriptors []*x.Type
	for _, route := range eligible {
		identity, err := d.derivedIdentity(component, route, len(eligible))
		if err != nil {
			return nil, nil, err
		}
		if err = d.reserve(identity); err != nil {
			return nil, nil, err
		}
		contract, ok := source.Input.ForRoute(spec.RouteRef{Method: route.Method, Path: route.Path})
		if !ok {
			return nil, nil, fmt.Errorf("report source route %s has no exact input contract", d.routeIdentity(route))
		}
		metadata, err := compileMetadata(component, contract)
		if err != nil {
			return nil, nil, fmt.Errorf("compile report %s: %w", identity.route.String(), err)
		}
		inputName := identity.typeName
		if metadata.settings.LinkedInputType != "" {
			inputName = metadata.settings.LinkedInputType
		}
		input, err := (&inputCompiler{
			metadata: metadata, component: component, typeName: inputName, packagePath: d.reportPackage(component),
			types: d.compiler.config.Types, authority: d.compiler.config.Authority,
		}).compile()
		if err != nil {
			return nil, nil, fmt.Errorf("compile report input %s: %w", identity.typeName, err)
		}
		plan := &Plan{
			target: exec.ComponentTarget{Component: component.Key, Route: contract.Route()},
			view:   component.RootView.CanonicalName(), inputType: input.typeOf, outputType: source.OutputType,
			dimensions: input.dimensions, measures: input.measures, filters: input.filters,
			orderIndex: input.orderIndex, limitIndex: input.limitIndex, offsetIndex: input.offsetIndex,
			holderByName: cloneHolders(metadata.holders),
		}
		derived := &Derived{
			Component: d.derivedComponent(component, route, identity, metadata, input),
			InputType: input.typeOf, OutputType: source.OutputType, Plan: plan, Handler: NewHandler(plan), Type: input.descriptor,
		}
		result = append(result, derived)
		if metadata.settings.LinkedInputType == "" {
			descriptors = append(descriptors, input.descriptor)
		}
		d.commit(identity)
		composed, descriptor, composeErr := d.deriveCompose(source, route, identity, metadata, plan)
		if composeErr != nil {
			return nil, nil, composeErr
		}
		if composed != nil {
			result = append(result, composed)
			descriptors = append(descriptors, descriptor)
		}
	}
	return result, descriptors, nil
}

func (d *reportDeriver) reserve(identity reportIdentity) error {
	if d.keys[identity.key.String()] {
		return fmt.Errorf("derived report component key collision: %s", identity.key.String())
	}
	if owner := d.routes[identity.route.String()]; owner != "" {
		return fmt.Errorf("derived report route %s collides with component %s", identity.route.String(), owner)
	}
	return nil
}

func (d *reportDeriver) commit(identity reportIdentity) {
	d.keys[identity.key.String()] = true
	d.routes[identity.route.String()] = identity.key.String()
}
