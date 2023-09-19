package translator

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
)

func (s *Service) detectComponentViewType(cache discover.Columns, resource *Resource) {
	if resource.Rule.IsGeneratation {
		return
	}
	root := resource.Rule.RootViewlet()
	if len(cache.Items) == 0 || len(root.View.With) > 0 || root.View.Self != nil || len(resource.OutputState) == 0 {
		return
	}

	if len(cache.Items) > 0 && root.TypeDefinition != nil {
		rootView := root.View.View
		relations := s.updateViewSchema(rootView.Caser, &rootView, resource, cache)

		fn := view.ColumnsSchema(rootView.Caser, root.Columns, relations, &rootView)
		schema, err := fn()
		if err != nil {
			s.Repository.Messages.AddWarning(root.Name, "detection", fmt.Sprintf("unable detect component view type: %v", err))
			return
		}
		root.TypeDefinition.DataType = schema.String()
		root.TypeDefinition.Fields = nil
		if root.View.View.Schema == nil {
			root.View.View.Schema = &state.Schema{Cardinality: state.Many}
		}
		if root.View.Schema.Cardinality == "" {
			root.View.Schema.Cardinality = state.Many
		}
		root.View.View.Schema.Name = "*" + root.TypeDefinition.Name
	}
}

func (s *Service) updateViewSchema(caser format.Case, aView *view.View, resource *Resource, cache discover.Columns) []*view.Relation {
	var relations []*view.Relation
	for i := range aView.With {
		rel := aView.With[i]
		of := *rel.Of
		rel.Of = &of
		relViewlet := resource.Rule.Viewlets.Lookup(rel.Of.View.Ref)
		relView := &relViewlet.View.View
		rel.Of.View = *relView
		rel.Of.View.Columns = cache.Items[rel.Of.View.Name]
		relations = append(relations, rel)
		rel.Of.View.With = s.updateViewSchema(caser, relView, resource, cache)
	}
	columns := cache.Items[aView.Name]

	fn := view.ColumnsSchema(caser, columns, relations, aView)
	schema, err := fn()
	if err != nil {
		s.Repository.Messages.AddWarning(aView.Name, "detection", fmt.Sprintf("unable detect component view type: %v", err))
		return relations
	}
	aView.Schema.SetType(schema)
	return relations
}

func (s *Service) detectColumns(resource *Resource, columnDiscovery discover.Columns) (err error) {
	var hasSummary bool
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		if viewlet.View.Template != nil && viewlet.View.Template.Summary != nil {
			hasSummary = true
		}
		if columns := viewlet.Spec.Columns; len(columns) > 0 {
			viewlet.Columns = view.NewColumns(columns)
			columnDiscovery.Items[viewlet.Name] = viewlet.Columns
			//TODO add meta column generation for SUMMARY/Meta tempalte
		}
		s.updateViewOutputType(viewlet, true)
		return nil
	}); err != nil {
		return err
	}
	if !hasSummary && !resource.Rule.IsGeneratation { //TODO add support
		err = s.persistViewMetaColumn(columnDiscovery, resource)
		if err != nil {
			return err
		}
	}
	return err
}
