package translator

import (
	"fmt"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"path"
)

type (
	View struct {
		Namespace string
		view.View
		build            bool
		ExecKind         string                 `json:",omitempty"`
		FetchRecords     bool                   `json:",omitempty"`
		Connector        string                 `json:",omitempty"`
		Self             *view.SelfReference    `json:",omitempty"`
		Warmup           map[string]interface{} `json:",omitempty"`
		Auth             string                 `json:",omitempty"`
		DataType         string                 `json:",omitempty"`
		AsyncTableName   string                 `json:",omitempty"`
		ParameterDerived bool
		CriteriaParam    string `json:",omitempty"`
		Cardinality      string
	}
)

func (v *View) applyHintSettings(namespace *Viewlet) error {
	viewJSONHint := namespace.ViewJSONHint
	if viewJSONHint == "" {
		return nil
	}
	err := inference.TryUnmarshalHint(viewJSONHint, &v)
	v.Namespace = namespace.Name
	if err != nil {
		return fmt.Errorf("invalid view %v hint, %w, %s", v, err, viewJSONHint)
	}

	v.applyShorthands(namespace)
	return nil
}

func (v *View) applyShorthands(viewlet *Viewlet) {

	if v.Self != nil {
		v.SelfReference = v.Self
	}
	if v.Cardinality != "" {
		if v.Schema == nil {
			v.Schema = &state.Schema{}
		}
		v.Schema.Cardinality = state.Cardinality(v.Cardinality)
	}

	if v.ExecKind != "" {

	}
	if v.FetchRecords {

	}
	if v.Connector != "" {
		viewlet.Connector = v.Connector
	}
	if v.Auth != "" {
		viewlet.Resource.State.Append(parser.DefaultOAuthParameter(v.Auth))
	}

	if v.DataType != "" {
		if v.Schema == nil {
			v.Schema = &state.Schema{}
		}
		setter.SetStringIfEmpty(&v.Schema.DataType, v.DataType)
	}

	if v.AsyncTableName != "" {

	}

	if len(v.Warmup) > 0 {
		v.View.Cache.Warmup = v.buildCacheWarmup(v.Warmup, viewlet)
	}
}

func (v *View) buildCacheWarmup(warmup map[string]interface{}, viewlet *Viewlet) *view.Warmup {
	if warmup == nil || viewlet.Join == nil {
		return nil
	}
	warmup = copyWarmup(warmup)

	_, refColumn := inference.ExtractRelationColumns(viewlet.Join)
	result := &view.Warmup{
		IndexColumn: refColumn,
	}

	multiSet := &view.CacheParameters{}
	for k, v := range warmup {
		switch actual := v.(type) {
		case []interface{}:
			multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: actual})
		default:
			multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: []interface{}{actual}})
		}
	}

	result.Cases = append(result.Cases, multiSet)
	return result
}

func copyWarmup(warmup map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for aKey := range warmup {
		if aKey == "" {
			continue
		}

		result[aKey] = warmup[aKey]
	}
	return result

}

// buildView builds View
func (v *View) buildView(rule *Rule, mode view.Mode) error {
	if v.build {
		return nil
	}
	v.build = true
	namespace := rule.Viewlets.Lookup(v.Namespace)

	if namespace.Table != nil {
		v.Table = namespace.Table.Name
	}
	if v.Mode == "" {
		v.Mode = mode
	}
	v.View.Connector = view.NewRefConnector(namespace.Connector)

	v.buildTemplate(namespace, rule)
	if v.Mode == view.ModeQuery {
		v.buildSelector(namespace, rule)
		v.buildColumnConfig(namespace)
		if err := v.buildRelations(namespace, rule); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) defaultLimit(isRoot bool) int {
	if v.ParameterDerived {
		return 1000 //data view base viewx
	}
	if isRoot {
		return 25
	}
	return 40
}

func (v *View) buildSelector(namespace *Viewlet, rule *Rule) {
	isRoot := rule.Root == v.Name
	if v.Selector == nil {
		v.Selector = &view.Config{}
	}

	selector := v.Selector
	selector.Namespace = rule.selectorNamespace(v.Name)
	setter.SetIntIfZero(&selector.Limit, v.defaultLimit(isRoot))
	if selector.Constraints == nil {
		selector.Constraints = &view.Constraints{
			Criteria:   true,
			Limit:      true,
			Offset:     true,
			Projection: true,
		}
		if !v.ParameterDerived {
			selector.Constraints.Filterable = []string{"*"}
		}
	}

	if v.CriteriaParam != "" {
		selector.CriteriaParameter = state.NewRefParameter(v.CriteriaParam)
	}

}

func (v *View) buildColumnConfig(namespace *Viewlet) {
	v.Exclude = namespace.Exclude
	//TODO add tags, formats, etc ...
	v.ColumnsConfig = map[string]*view.ColumnConfig{}
	for i, config := range namespace.ColumnConfig {
		v.ColumnsConfig[config.Name] = namespace.ColumnConfig[i]
	}
	for k, tag := range namespace.Tags {
		config, ok := v.ColumnsConfig[k]
		if !ok {
			config = &view.ColumnConfig{Name: k}
			v.ColumnsConfig[k] = config
		}
		config.Tag = &tag
	}
}

func (v *View) buildTemplate(namespace *Viewlet, rule *Rule) {
	isRoot := rule.Root == v.Name
	resource := namespace.Resource
	v.EnsureTemplate()
	v.Template.Source = namespace.SanitizedSQL
	v.Template.Parameters = v.matchParameters(namespace.SanitizedSQL, resource.State, isRoot)
}

// matchParameters matches parameter used by SQL, and add explicit parameter for root view
func (v *View) matchParameters(SQL string, aState inference.State, root bool) []*state.Parameter {
	var result []*state.Parameter
	SQLState := aState.StateForSQL(SQL, root)
	for _, candidate := range SQLState {
		result = append(result, state.NewRefParameter(candidate.Name))
	}
	return result
}

func (v *View) buildRelations(parentNamespace *Viewlet, rule *Rule) error {
	if parentNamespace.Spec.Relations == nil {
		return nil
	}
	for _, relation := range parentNamespace.Spec.Relations {
		relNamespace := rule.Viewlets.Lookup(relation.Namespace)
		if err := relNamespace.View.buildView(rule, view.ModeQuery); err != nil {
			return err
		}
		//TODO double check rel name uniqness
		viewRelation := &view.Relation{Name: relation.Name}
		if relation.ParentField == nil {
			return fmt.Errorf("failed to add relation: %v, unknown holder", relation.Name)
		}
		if relation.KeyField == nil {
			return fmt.Errorf("failed to add relation: %v, unknown reference", relation.Name)
		}
		viewRelation.Column = relation.ParentField.Column.Name
		viewRelation.ColumnNamespace = relation.ParentField.Column.Namespace
		viewRelation.Field = relation.ParentField.Name
		holderFormat, err := format.NewCase(formatter.DetectCase(relNamespace.Name))
		if err != nil {
			return err
		}
		viewRelation.Holder = holderFormat.Format(relNamespace.Name, format.CaseUpperCamel)
		viewRelation.IncludeColumn = true
		relNamespace.Holder = viewRelation.Holder
		refViewName := relNamespace.View.Name
		refColumn := relation.KeyField.Column.Name
		refField := ""
		refField = relation.KeyField.Name
		viewRelation.Of = view.NewReferenceView(refViewName, refViewName+"#", refColumn, refField)
		viewRelation.Cardinality = relation.Cardinality
		v.View.With = append(v.View.With, viewRelation)
	}
	return nil
}

func (v *View) GenerateFiles(baseURL string, ruleName string, files *asset.Files) {
	if v.View.Template.Source != "" {
		file := asset.NewFile(path.Join(baseURL, ruleName, v.Namespace+".sql"), v.View.Template.Source)
		files.Append(file)
		v.View.Template.SourceURL = path.Join(ruleName, v.Namespace+".sql")
		v.View.Template.Source = ""
	}
}
