package translator

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
)

func (s *Service) detectComponentViewType(cache discover.Columns, resource *Resource) {
	if resource.Rule.IsGeneratation {
		return
	}
	root := resource.Rule.RootViewlet()
	//TODO remove with, OutputState check and fix it
	template := root.View.Template
	if len(cache.Items) == 0 || len(root.View.With) > 0 || root.View.Self != nil || (template != nil && template.Summary != nil) {
		return
	}

	if len(cache.Items) > 0 && root.TypeDefinition != nil {
		rootView := view.View{}
		//TODO understand implcation of not cloning
		if data, err := json.Marshal(root.View.View); err == nil {
			_ = json.Unmarshal(data, &rootView)
		}
		rootViewlet := resource.Rule.RootViewlet()
		_, err := s.updateViewSchema(&rootView, resource, cache, rootViewlet.typeRegistry)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}

		root.TypeDefinition.DataType = rootView.Schema.CompType().String()
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

func (s *Service) detectViewCaser(columns view.Columns) (format.Case, error) {
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	caseFormat := formatter.CaseFormat(formatter.DetectCase(columnNames...))
	if err := caseFormat.Init(); err != nil {
		return 0, err
	}
	caser, err := caseFormat.Caser()
	return caser, err
}

func (s *Service) updateViewSchema(aView *view.View, resource *Resource, cache discover.Columns, registry *xreflect.Types) ([]*view.Relation, error) {
	var relations []*view.Relation
	var err error

	fmt.Printf("SCHEMA: %s\n", aView.Schema.TypeName())

	for i := range aView.With {
		rel := aView.With[i]
		of := *rel.Of
		rel.Of = &of
		relViewlet := resource.Rule.Viewlets.Lookup(rel.Of.View.Ref)
		relView := &relViewlet.View.View
		rel.Of.View = *relView
		relations = append(relations, rel)
		if _, err = s.updateViewSchema(relView, resource, cache, registry); err != nil {
			return nil, err
		}
	}

	columns := cache.Items[aView.Name]
	caser, err := s.detectViewCaser(columns)
	if err != nil {
		return nil, fmt.Errorf("invalud view %scaser: %w", aView.Name, err)
	}

	if len(aView.ColumnsConfig) > 0 {

		for _, column := range columns {
			if cfg, ok := aView.ColumnsConfig[column.Name]; ok {
				columnType := column.DataType
				column.ApplyConfig(cfg)
				if column.DataType != columnType {
					rType, err := types.LookupType(registry.Lookup, column.DataType)
					if err != nil {
						return nil, fmt.Errorf("failed to update column: %v %w", column.Name, err)
					}
					column.SetColumnType(rType)
				}
			}
		}
	}

	fn := view.ColumnsSchema(caser, columns, relations, aView)
	schema, err := fn()
	if err != nil {
		s.Repository.Messages.AddWarning(aView.Name, "detection", fmt.Sprintf("unable detect component view type: %v", err))
		return relations, nil
	}
	aView.Schema.SetType(schema)
	return relations, nil
}

func (s *Service) detectColumns(resource *Resource, columnDiscovery discover.Columns) (err error) {
	var hasSummary bool
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		if viewlet.View.Template != nil && viewlet.View.Template.Summary != nil {
			hasSummary = true
		}
		if columns := viewlet.Spec.Columns; len(columns) > 0 {
			viewlet.Columns = view.NewColumns(columns)

			isValid := true
			for _, candidate := range viewlet.Columns {
				if candidate.DataType == "" {
					s.Repository.Messages.AddWarning("detection", "view", fmt.Sprintf("view: %v column: %v, unable to detect column type", viewlet.Name, candidate.Name))
					isValid = false
				}
			}
			if isValid {
				columnDiscovery.Items[viewlet.Name] = viewlet.Columns
			} else {
				fmt.Printf("faild to run column detectrion for %s\n", viewlet.Name)
			}
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
