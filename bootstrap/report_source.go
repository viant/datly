package bootstrap

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	readercompiler "github.com/viant/datly/sql/reader/compiler"
)

// ReportSourceComponent returns detached projection metadata for report
// derivation. Relation tags are resolved by the reader compiler, not necessarily
// present in the authored spec. This snapshot does not replace execution plans.
func (a *Artifact) ReportSourceComponent() *spec.Component {
	if a == nil || a.Component == nil {
		return nil
	}
	if a.reportSource != nil {
		return a.reportSource.Clone()
	}
	result := a.Component.Clone()
	if result.RootView == nil || a.Reader == nil || a.Reader.Root == nil {
		return result
	}
	return reportComponentWithView(result, a.Reader.Root.View)
}

func reportComponentWithView(result *spec.Component, root *data.View) *spec.Component {
	result.RootView.Source = root.Spec.Source.Clone()
	result.RootView.Relations = nil
	for _, relation := range root.Relations {
		if relation.IsOutput() {
			continue
		}
		projected := &spec.Relation{
			Name: relation.Name, Kind: relation.Kind, Holder: relation.Holder,
			Cardinality: relation.Cardinality, View: relation.Of.View.Spec.Clone(),
		}
		for i, parent := range relation.On {
			child := relation.Of.On[i]
			projected.On = append(projected.On, &spec.RelationLink{
				ParentNamespace: parent.Namespace, ParentColumn: parent.OutputColumn(),
				ChildNamespace: child.Namespace, ChildColumn: child.OutputColumn(),
			})
		}
		result.RootView.Relations = append(result.RootView.Relations, projected)
	}
	return result
}

func (c *artifactCompiler) compileHandlerReportSource(component *spec.Component) (*spec.Component, error) {
	// Resolve output declarations only on the detached report snapshot. The
	// handler's component, output binding and execution ownership stay untouched.
	source, err := (ContractResolver{Component: component, OutputType: linkedContractType(c.input.OutputType)}).Resolve()
	if err != nil {
		return nil, err
	}
	if source.RootView == nil {
		return source, nil
	}
	if err = c.compileOutputColumns(source); err != nil {
		return nil, err
	}
	view, err := readercompiler.CompileViewMetadata(readercompiler.Input{
		Component: source, OutputType: c.input.OutputType, DirectViewField: c.input.DirectViewField,
		Const: c.input.Const, Resources: c.input.Resources,
	})
	if err != nil {
		return nil, err
	}
	return reportComponentWithView(source, view), nil
}
