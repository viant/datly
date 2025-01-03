package translator

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"strings"
)

func (s *Service) detectComponentViewType(viewColumns discover.Columns, resource *Resource) {
	if resource.Rule.IsGeneratation {
		return
	}
	root := resource.Rule.RootViewlet()
	//TODO remove with, OutputState check and fix it
	if len(viewColumns.Items) == 0 || root.TypeDefinition == nil {
		return
	}

	rootViewTypeName := root.View.Schema.Name
	pkg := root.View.Schema.Package
	setter.SetStringIfEmpty(&pkg, resource.rule.Package())
	if index := strings.LastIndex(rootViewTypeName, "."); index != -1 {
		pkg = rootViewTypeName[:index]
		rootViewTypeName = rootViewTypeName[index+1:]
	}

	viewType, _ := resource.typeRegistry.Lookup(rootViewTypeName, xreflect.WithPackage(pkg))
	if viewType != nil {
		root.View.View.Schema.Name = rootViewTypeName
		root.View.View.Schema.DataType = "*" + rootViewTypeName
		root.View.View.Schema.Package = pkg
		return
	}

	cloneRoot := view.View{}
	if data, err := json.Marshal(root.View.View); err == nil {
		_ = json.Unmarshal(data, &cloneRoot)
	}
	cloneRoot.SetResource(&resource.Resource)
	var types []*xreflect.Type

	if err := s.updateViewSchema(&cloneRoot, resource, viewColumns, resource.typeRegistry, &types, resource.Rule.Doc.Columns); err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}

	setter.SetStringIfEmpty(&rootViewTypeName, view.DefaultTypeName(root.View.Name))

	rootType := MatchByName(types, rootViewTypeName)
	if rootType != nil {
		root.TypeDefinition.DataType = rootType.Body()
		root.TypeDefinition.Name = rootType.Name
		root.TypeDefinition.Fields = nil
	}

	if root.View.View.Schema == nil {
		root.View.View.Schema = &state.Schema{Cardinality: state.Many}
	}

	if dataParameter := resource.OutputState.Parameters().LookupByLocation(state.KindOutput, "view"); dataParameter != nil {
		if dataParameter.Schema != nil && dataParameter.Schema.Cardinality != "" {
			root.View.Schema.Cardinality = dataParameter.Schema.Cardinality
		}
	}

	if root.View.Schema.Cardinality == "" {
		root.View.Schema.Cardinality = state.Many
	}
	root.View.View.Schema.DataType = "*" + root.TypeDefinition.Name
	root.View.View.Schema.SetPackage(resource.rule.Package())
}

func MatchByName(types []*xreflect.Type, name string) *xreflect.Type {
	for _, candidate := range types {
		if candidate.Name == name {
			return candidate
		}
	}
	return nil
}

func (s *Service) detectColumnCaseFormat(columns view.Columns) (text.CaseFormat, error) {
	var columnNames []string
	for _, column := range columns {
		if strings.Contains(strings.ToLower(column.Tag), "ignorecaseformatter") {
			continue
		}
		columnNames = append(columnNames, column.Name)
	}
	caseFormat := text.DetectCaseFormat(columnNames...)
	if !caseFormat.IsDefined() {
		return "", fmt.Errorf("failed to detect case format for: %v", columnNames)
	}
	return caseFormat, nil
}

func (s *Service) updateViewSchema(aView *view.View, resource *Resource, cache discover.Columns, registry *xreflect.Types, types *[]*xreflect.Type, doc state.Documentation) (err error) {
	if aView.Schema != nil && (aView.Schema.Name != "" && aView.Schema.Name != "string") {
		return nil
	}
	var relations []*view.Relation
	for i := range aView.With {
		rel := aView.With[i]
		of := *rel.Of
		rel.Of = &of
		relViewlet := resource.Rule.Viewlets.Lookup(rel.Of.View.Ref)
		relView := &relViewlet.View.View
		relSchema := relView.Schema
		relView.SetResource(aView.GetResource())
		rel.Of.View = *relView
		relations = append(relations, rel)
		if relSchema != nil && relSchema.Name != "" { //used has defined custom type, skip generation
			continue
		}
		relSchema.Name = view.DefaultTypeName(relSchema.Name)
		if err = s.updateViewSchema(relView, resource, cache, registry, types, doc); err != nil {
			return err
		}
	}

	columns := cache.Items[aView.Name]
	if err = columns.ApplyConfig(aView.ColumnsConfig, registry.Lookup); err != nil {
		return err
	}
	caseFormat, err := s.detectColumnCaseFormat(columns)
	if err != nil {
		return fmt.Errorf("invalud view %scaser: %w", aView.Name, err)
	}

	aViewlet := resource.Rule.Viewlets.Lookup(aView.Name)
	if aViewlet.Summary != nil {
		summarySchema, err := s.updateSummarySchema(resource, aView, caseFormat, aViewlet)
		if err != nil {
			return err
		}
		if summaryParameter := resource.OutputState.Parameters().LookupByLocation(state.KindOutput, "summary"); summaryParameter != nil {
			summaryParameter.Schema = summarySchema
		}
	}
	fn := view.ColumnsSchemaDocumented(caseFormat, columns, relations, aView, doc)
	schemaType, err := fn()
	if err != nil {
		s.Repository.Messages.AddWarning(aView.Name, "detection", fmt.Sprintf("unable detect component view type: %v", err))
		return nil
	}
	aView.Schema.SetType(schemaType)
	aView.Schema.Name = view.DefaultTypeName(aView.Name)
	pkg := resource.rule.Package()
	aView.Schema.SetPackage(pkg)
	rType := aView.Schema.CompType()
	viewType := xreflect.NewType(view.DefaultTypeName(aView.Name), xreflect.WithPackage(pkg), xreflect.WithReflectType(rType))
	*types = append(*types, viewType)
	typeDef := &view.TypeDefinition{Name: viewType.Name, Package: pkg, DataType: viewType.Body()}
	resource.AppendTypeDefinition(typeDef)
	return nil
}

func (s *Service) updateSummarySchema(resource *Resource, aView *view.View, caser text.CaseFormat, aViewlet *Viewlet) (*state.Schema, error) {
	summary := aView.Template.Summary
	if summary.Schema == nil {
		summary.Schema = &state.Schema{}
	}
	if summary.Schema.Type() == nil {
		buildSummarySchema := view.ColumnsSchema(caser, aViewlet.Summary.Columns, nil, &aViewlet.View.View, resource.Rule.Doc.Columns)
		summaryType, err := buildSummarySchema()
		if err != nil {
			return nil, fmt.Errorf("failed to build summary view %v schema %w", summary.Name, err)
		}
		summary.Schema.SetType(summaryType)
	}
	pkg := resource.rule.Package()
	rType := summary.Schema.CompType()
	summaryType := xreflect.NewType(view.DefaultTypeName(summary.Name), xreflect.WithPackage(pkg), xreflect.WithReflectType(rType))

	summary.Schema.DataType = "*" + summaryType.Name
	summary.Schema.Package = pkg
	resource.AppendTypeDefinition(&view.TypeDefinition{Name: summaryType.Name, Package: pkg, DataType: summaryType.Body()})
	return summary.Schema, nil
}

func (s *Service) detectColumns(resource *Resource, columnDiscovery discover.Columns) (err error) {
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		if len(viewlet.Columns) > 0 {
			return nil
		}
		if columns := viewlet.Spec.Columns; len(columns) > 0 {
			viewlet.Columns = view.NewColumns(columns).Dedupe()
			isValid := s.ensureValidColumns(viewlet)
			if isValid && !viewlet.IsSummary {
				columnDiscovery.Items[viewlet.Name] = viewlet.Columns.Dedupe()
			}
			summary := viewlet.Summary
			if summary != nil && len(summary.Spec.Columns) > 0 {
				if len(summary.Columns) == 0 {
					summary.Columns = view.NewColumns(summary.Spec.Columns).Dedupe()
				}
				key := view.SummaryViewKey(viewlet.View.Name, summary.View.Name)
				columnDiscovery.Items[key] = summary.Columns.Dedupe()
			}
		}
		s.updateViewOutputType(viewlet, true, resource.Rule.Doc.Columns)
		return nil
	}); err != nil {
		return err
	}
	if !resource.Rule.IsGeneratation && resource.Rule.IsReader() { //skip view column generation if generator use translator
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
	columnConfig := viewlet.ColumnConfig.Index()

	for _, candidate := range viewlet.Columns {
		if config, ok := columnConfig[candidate.Name]; ok {
			if config.Tag != nil {
				candidate.Tag = *config.Tag
			}
			if config.DataType != nil {
				candidate.DataType = *config.DataType
				rType, _ := viewlet.Resource.typeRegistry.Lookup(candidate.DataType)
				if rType != nil {
					candidate.SetColumnType(rType)
				}
				typeName := strings.ReplaceAll(strings.ReplaceAll(*config.DataType, "*", ""), "[]", "")
				candidate.Tag += fmt.Sprintf(` typeName:"%s"`, typeName)
			}
		}

		if candidate.DataType == "" {
			s.Repository.Messages.AddWarning("detection", "view", fmt.Sprintf("view: %v column: %v, unable to detect column type", viewlet.Name, candidate.Name))
			isValid = false
			break
		}
	}
	return isValid
}
