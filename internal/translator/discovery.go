package translator

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"strings"
)

func (s *Service) detectComponentViewType(cache discover.Columns, resource *Resource) {
	if resource.Rule.IsGeneratation {
		return
	}
	root := resource.Rule.RootViewlet()
	//TODO remove with, OutputState check and fix it
	if len(cache.Items) == 0 || root.TypeDefinition == nil || (root.View.Template != nil && root.View.Template.Summary != nil) {
		return
	}

	cloneRoot := view.View{}
	//TODO understand implication of not cloning
	if data, err := json.Marshal(root.View.View); err == nil {
		_ = json.Unmarshal(data, &cloneRoot)
	}
	rootViewlet := resource.Rule.RootViewlet()
	_, err := s.updateViewSchema(&cloneRoot, resource, cache, rootViewlet.typeRegistry)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}

	root.TypeDefinition.DataType = cloneRoot.Schema.CompType().String()
	root.TypeDefinition.Fields = nil
	if root.View.View.Schema == nil {
		root.View.View.Schema = &state.Schema{Cardinality: state.Many}
	}
	if root.View.Schema.Cardinality == "" {
		root.View.Schema.Cardinality = state.Many
	}
	root.View.View.Schema.Name = "*" + root.TypeDefinition.Name

	if cloneRoot.Template != nil && cloneRoot.Template.Summary != nil {
		summarySchema := cloneRoot.Template.Summary.Schema
		if summarySchema.Type() != nil && summarySchema.Name == "" {
			summary := root.View.Template.Summary
			summaryTypeName := summary.Name + "Output"
			summarySchema.Name = "*" + summaryTypeName
			summary.Schema = summarySchema
			resource.AppendTypeDefinition(&view.TypeDefinition{Name: summaryTypeName, DataType: summary.Schema.Type().String()})
		}
	}
}

func (s *Service) detectViewCaser(columns view.Columns) (format.Case, error) {
	var columnNames []string
	for _, column := range columns {
		if strings.Contains(strings.ToLower(column.Tag), "ignorecaseformatter") {
			continue
		}
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
	if err = columns.ApplyConfig(aView.ColumnsConfig, registry.Lookup); err != nil {
		return nil, err
	}

	caser, err := s.detectViewCaser(columns)
	if err != nil {
		return nil, fmt.Errorf("invalud view %scaser: %w", aView.Name, err)
	}

	aViewlet := resource.Rule.Viewlets.Lookup(aView.Name)
	if aViewlet.Summary != nil {
		summary := aView.Template.Summary
		if summary.Schema == nil {
			summary.Schema = &state.Schema{}
		}
		if summary.Schema.Type() == nil {
			//dataType := summary.Schema.TypeName()
			buildSummarySchema := view.ColumnsSchema(caser, aViewlet.Summary.Columns, nil, &aViewlet.View.View)
			summaryType, err := buildSummarySchema()
			if err != nil {
				return nil, fmt.Errorf("failed to build summary view %v schema %w", summary.Name, err)
			}
			summary.Schema.SetType(summaryType)
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
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		if len(viewlet.Columns) > 0 {
			return nil
		}
		if columns := viewlet.Spec.Columns; len(columns) > 0 {
			viewlet.Columns = view.NewColumns(columns)
			isValid := s.ensureValidColumns(viewlet)
			if isValid && !viewlet.IsSummary {
				columnDiscovery.Items[viewlet.Name] = viewlet.Columns
			}
			summary := viewlet.Summary
			if summary != nil && len(summary.Spec.Columns) > 0 {
				if len(viewlet.Columns) == 0 {
					viewlet.Columns = view.NewColumns(summary.Spec.Columns)
				}
				key := view.SummaryViewKey(viewlet.View.Name, summary.View.Name)
				columnDiscovery.Items[key] = viewlet.Columns
			}
		}
		s.updateViewOutputType(viewlet, true)
		return nil
	}); err != nil {
		return err
	}

	if !resource.Rule.IsGeneratation { //skip view column generation if generator use translator
		err = s.persistViewMetaColumn(columnDiscovery, resource)
		if err != nil {
			return err
		}
	}
	return err
}

// ensureValidColumns checks if all column have detected data type
func (s *Service) ensureValidColumns(viewlet *Viewlet) bool {
	isValid := true
	for _, candidate := range viewlet.Columns {
		if candidate.DataType == "" {
			s.Repository.Messages.AddWarning("detection", "view", fmt.Sprintf("view: %v column: %v, unable to detect column type", viewlet.Name, candidate.Name))
			isValid = false
			break
		}
	}
	return isValid
}
