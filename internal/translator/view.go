package translator

import (
	"fmt"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"path"
	"strings"
)

type (
	View struct {
		Namespace string
		view.View
		build          bool
		ExecKind       string                 `json:",omitempty"`
		FetchRecords   bool                   `json:",omitempty"`
		Connector      string                 `json:",omitempty"`
		Self           *view.SelfReference    `json:",omitempty"`
		Warmup         map[string]interface{} `json:",omitempty"`
		Auth           string                 `json:",omitempty"`
		DataType       string                 `json:",omitempty"`
		AsyncTableName string                 `json:",omitempty"`
	}
)

func (v *View) applyHintSettings(namespace *Namespace) error {
	viewJSONHint := namespace.ViewJSONHint
	if viewJSONHint == "" {
		return nil
	}
	err := parser.TryUnmarshalHint(viewJSONHint, &v)
	v.Namespace = namespace.Name
	if err != nil {
		return fmt.Errorf("invalid view %v hint, %w, %s", v, err, viewJSONHint)
	}
	v.applyShorthands(namespace)
	return nil
}

func (v *View) applyShorthands(namespace *Namespace) {

	if v.Self != nil {
		v.SelfReference = v.Self
	}

	if v.ExecKind != "" {

	}
	if v.FetchRecords {

	}
	if v.Connector != "" {
		namespace.Connector = v.Connector
	}
	if v.Auth != "" {
		namespace.Resource.State.Append(parser.DefaultOAuthParameter(v.Auth))
	}

	if v.DataType != "" {

	}

	if v.AsyncTableName != "" {

	}

	if len(v.Warmup) > 0 {

	}
}

//buildView builds View
func (v *View) BuildView(rule *Rule) error {
	if v.build {
		return nil
	}
	v.build = true

	namespace := rule.Namespaces.Lookup(v.Namespace)
	v.Table = namespace.Table.Name
	v.Mode = view.ModeQuery
	v.View.Connector = view.NewRefConnector(v.Connector)
	v.buildSelector(namespace, rule)
	v.buildTemplate(namespace, rule)
	v.buildColumnConfig(namespace)
	if err := v.buildRelations(namespace, rule); err != nil {
		return err
	}
	return nil
}

func (v *View) defaultLimit(isRoot bool) int {
	if isRoot {
		return 25
	}
	return 40
}

func (v *View) buildSelector(namespace *Namespace, rule *Rule) {
	isRoot := rule.Root == v.Name
	if v.Selector == nil {
		v.Selector = &view.Config{}
	}

	selector := v.Selector
	selector.Namespace = rule.selectorNamespace(v.Name)
	setter.SetIntIfZero(&selector.Limit, v.defaultLimit(isRoot))
	if selector.Constraints == nil {
		selector.Constraints = &view.Constraints{
			Filterable: []string{"*"},
			Criteria:   true,
			Limit:      true,
			Offset:     true,
			Projection: true,
		}
	}

}

func (v *View) buildColumnConfig(namespace *Namespace) {
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

func (v *View) buildTemplate(namespace *Namespace, rule *Rule) {
	isRoot := rule.Root == v.Name
	resource := namespace.Resource
	v.Template = &view.Template{Source: namespace.SanitizedSQL}
	v.matchParameters(namespace.SanitizedSQL, resource.State, isRoot)
}

//matchParameters matches parameter used by SQL, and add explicit parameter for root view
func (v *View) matchParameters(SQL string, state inference.State, root bool) []*view.Parameter {
	var result []*view.Parameter
	for _, candidate := range state {
		if (root && candidate.Explicit) || usesParameter(SQL, candidate.Name) {
			result = append(result, view.NewRefParameter(candidate.Name))
		}
	}
	return result
}

func (v *View) buildRelations(parentNamespace *Namespace, rule *Rule) error {
	if parentNamespace.Spec.Relations == nil {
		return nil
	}
	for _, relation := range parentNamespace.Spec.Relations {
		relNamespace := rule.Namespaces.Lookup(relation.Namespace)
		if err := relNamespace.View.BuildView(rule); err != nil {
			return err
		}
		//TODO double check rel name uniqness
		viewRelation := &view.Relation{Name: relation.Name}
		if relation.ParentField == nil {
			return fmt.Errorf("faild to add relation: %v, unknown holder", relation.Name)
		}
		if relation.KeyField == nil {
			return fmt.Errorf("faild to add relation: %v, unknown reference", relation.Name)
		}
		viewRelation.Column = relation.ParentField.Column.Name
		viewRelation.Field = relation.ParentField.Name
		holderFormat, err := format.NewCase(formatter.DetectCase(relNamespace.Name))
		if err != nil {
			return err
		}
		viewRelation.Holder = holderFormat.Format(relNamespace.Name, format.CaseUpperCamel)
		relNamespace.Holder = viewRelation.Holder
		refViewName := relNamespace.View.Name
		refColumn := relation.KeyField.Column.Name
		refField := relation.KeyField.Name
		viewRelation.Of = view.NewReferenceView(refViewName, refViewName+"#", refColumn, refField)
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

func usesParameter(text string, name string) bool {
	if strings.Contains(text, "$"+name) {
		return true
	}
	if strings.Contains(text, "${"+name) {
		return true
	}
	if strings.Contains(text, "${"+name) {
		return true
	}
	if strings.Contains(text, "Unsafe."+name) {
		return true
	}
	return false
}
