package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/repository/async"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type (
	Rule struct {
		Viewlets

		CustomValidation           bool
		IgnoreEmptyQueryParameters bool
		orderNamespaces            []string
		Root                       string
		//TODO replace with repository.Component and path.Settings ?
		router.Route

		*contract.Output
		Async        *async.Config          `json:",omitempty"`
		Cache        *view.Cache            `json:",omitempty"`
		CSV          *content.CSVConfig     `json:",omitempty"`
		Const        map[string]interface{} `json:",omitempty"`
		ConstURL     string                 `json:",omitempty"`
		DocURL       string
		Doc          state.Docs
		RequestBody  *BodyConfig         `json:",omitempty"`
		ResponseBody *ResponseBodyConfig `json:",omitempty"`

		TypeSrc     *parser.TypeImport         `json:",omitempty"`
		Package     string                     `json:",omitempty"`
		Router      *RouterConfig              `json:",omitempty" yaml:",omitempty"`
		DataFormat  string                     `json:",omitempty"`
		TabularJSON *content.TabularJSONConfig `json:",omitempty"`
		XML         *content.XMLConfig         `json:",omitempty"`
		Type        string                     `json:",omitempty"`
		HandlerArgs []string                   `json:",omitempty"`
		InputType   string                     `json:",omitempty"`
		OutputType  string                     `json:",omitempty"`
		With        []string                   `json:",omitempty"`
		Include     []string                   `json:",omitempty"`
		indexNamespaces
		IsGeneratation    bool
		XMLUnmarshalType  string `json:",omitempty"`
		JSONUnmarshalType string `json:",omitempty"`

		OutputParameter *inference.Parameter
	}

	indexNamespaces []*indexNamespace

	indexNamespace struct {
		Name      string
		Namespace string
	}

	TypeImport struct {
		URL            string
		Types          []string
		Alias          string
		ForceGoTypeUse bool
	}

	RouterConfig struct {
		RouterURL string `json:",omitempty" yaml:",omitempty"`
		URL       string `json:",omitempty" yaml:",omitempty"`
		Routes    []struct {
			SourceURL string
		}
	}

	//depprecated
	BodyConfig struct {
		DataType string `json:",omitempty"`
	}

	//depreacted
	ResponseBodyConfig struct {
		From string
	}
)

func (r *Rule) StateTypePackage() string {
	if r.InputType == "" {
		return ""
	}
	index := strings.LastIndex(r.InputType, ".")
	if index == -1 {
		return ""
	}
	return r.InputType[:index]
}

func (r *Rule) applyGeneratorOutputSetting() {
	root := r.RootViewlet()
	outputConfig := root.OutputSettings
	setter.SetStringIfEmpty(&r.Route.Output.Field, outputConfig.Field)
	if r.Route.Output.Style == "" && r.Route.Output.Field != "" {
		r.Route.Output.Style = contract.ComprehensiveStyle
	}
	if r.Route.Output.Style == "" {
		r.Route.Output.Style = contract.Style(outputConfig.Style)
	}

	if r.Route.Output.Title == "" {
		r.Route.Output.Title = outputConfig.Title
	}

	if r.Route.Output.Cardinality == "" {
		r.Route.Output.Cardinality = outputConfig.ViewCardinality()
	}

}
func (r *Rule) DSQLSetting() interface{} {

	return struct {
		URI          string
		Method       string
		ResponseBody *ResponseBodyConfig `json:",omitempty"`
		Type         string              `json:",omitempty"`
		InputType    string              `json:",omitempty"`
		OutputType   string              `json:",omitempty"`
	}{
		URI:          r.URI,
		Method:       r.Method,
		ResponseBody: r.ResponseBody,
		Type:         r.Type,
		InputType:    r.InputType,
		OutputType:   r.OutputType,
	}
}

func (r *Rule) ShallGenerateHandler() bool {
	return r.Type != ""
}

func (r *Rule) IsMany() bool {
	return r.Route.Output.Cardinality == "" || r.Route.Output.Cardinality == state.Many
}

func (r *Rule) IsBasic() bool {
	return r.Route.Output.Style != contract.ComprehensiveStyle && r.Route.Output.Field == ""
}

func (r *Rule) ExtractSettings(dSQL *string) error {

	if index := strings.Index(*dSQL, "*/"); index != -1 {
		if err := shared.UnmarshalWithExt([]byte((*dSQL)[:index+2]), &r, ".json"); err != nil {
			return fmt.Errorf("failed to extract rule setting %w", err)
		}
		*dSQL = (*dSQL)[index+2:]
	}
	r.applyShortHands()
	return nil
}

func (r *Rule) GetField() string {
	if r.IsBasic() {
		return ""
	}

	if r.Field == "" {
		return "data"
	}

	return r.Field
}

func (r *Resource) initRule(ctx context.Context, fs afs.Service, dSQL *string) error {
	rule := r.Rule
	rule.applyDefaults()
	if err := r.loadData(ctx, fs, rule.ConstURL, &rule.Const); err != nil {
		r.messages.AddWarning(r.rule.RuleName(), "const", fmt.Sprintf("failed to load constant : %v %w", rule.ConstURL, err))
	}

	if err := r.loadData(ctx, fs, rule.DocURL, &rule.Doc); err != nil {
		r.messages.AddWarning(r.rule.RuleName(), "doc", fmt.Sprintf("failed to load documentation: %v due to the %v", rule.DocURL, err.Error()))
	}
	r.State.AppendConst(rule.Const)
	return nil
}

func (r *Resource) loadData(ctx context.Context, fs afs.Service, URL string, dest interface{}) error {
	if URL == "" {
		return nil
	}

	dataURL, err := r.assetURL(ctx, URL, fs)
	if err != nil || dataURL == "" {
		return err
	}
	data, err := fs.DownloadWithURL(ctx, dataURL)
	if err != nil {
		return err
	}
	data = []byte(r.Resource.Substitutes.Replace(string(data)))
	return shared.UnmarshalWithExt(data, dest, path.Ext(dataURL))
}

func (r *Resource) getConstantURL(ctx context.Context, rule *Rule, fs afs.Service) (string, error) {
	if rule.ConstURL == "" {
		return "", nil
	}

	return r.assetURL(ctx, rule.ConstURL, fs)
}

func (r *Resource) assetURL(ctx context.Context, ruleURL string, fs afs.Service) (string, error) {
	if !url.IsRelative(ruleURL) {
		return ruleURL, nil
	}

	wd, _ := os.Getwd()
	candidateURL := filepath.Join(wd, ruleURL)
	if ok, _ := fs.Exists(ctx, candidateURL); ok {
		return filepath.Join(wd, ruleURL), nil
	}

	candidateURL = filepath.Join(r.rule.SourceDirectory(), ruleURL)
	if ok, _ := fs.Exists(ctx, candidateURL); ok {
		return candidateURL, nil
	}

	candidateURL = filepath.Join(r.rule.ModuleLocation, ruleURL)
	if ok, _ := fs.Exists(ctx, candidateURL); ok {
		return candidateURL, nil
	}

	return filepath.Join(r.rule.BaseRuleURL(), ruleURL), nil
}

func (n indexNamespaces) Lookup(viewName string) *indexNamespace {
	for _, candidate := range n {
		if candidate.Name == viewName {
			return candidate
		}
	}
	return nil
}

func (n indexNamespaces) LookupNs(namespace string) *indexNamespace {
	for _, candidate := range n {
		if candidate.Namespace == namespace {
			return candidate
		}
	}
	return nil
}

func (r *Rule) selectorNamespace(viewName string) string {
	entry := r.indexNamespaces.Lookup(viewName)
	if entry != nil {
		return entry.Namespace
	}
	entry = &indexNamespace{Name: viewName}
	parts := strings.Split(strings.ToLower(viewName), "_")
	if len(parts) > 2 {
		return parts[len(parts)-2][0:1] + parts[len(parts)-1][0:1]
	}
	candidatePrefix := parts[len(parts)-1][0:2]
	if r.LookupNs(candidatePrefix) == nil {
		entry.Namespace = candidatePrefix
		r.indexNamespaces = append(r.indexNamespaces, entry)
		return candidatePrefix
	}
	for i := 1; i < len(entry.Name); i++ {
		candidate := candidatePrefix + entry.Name[i:i+1]
		if r.LookupNs(candidate) == nil {
			entry.Namespace = candidate
			return candidatePrefix
		}
	}
	return entry.Name
}

func (r *Rule) applyDefaults() {
	setter.SetStringIfEmpty(&r.Method, "GET")
	setter.SetCaseFormatIfEmpty(&r.Route.Output.CaseFormat, "lc")
	setter.SetBoolIfFalse(&r.EnableAudit, true)
	setter.SetBoolIfFalse(&r.Input.IgnoreEmptyQueryParameters, r.IgnoreEmptyQueryParameters)
	setter.SetBoolIfFalse(&r.Input.CustomValidation, r.CustomValidation || r.Type != "")
	if r.XMLUnmarshalType != "" {
		r.Route.Content.Marshaller.XML.TypeName = r.XMLUnmarshalType
	}
	if r.Route.Cors == nil {
		dC := dpath.DefaultCors()
		r.Route.Cors = dC
	}
}

func (r *Rule) RootViewlet() *Viewlet {
	return r.Viewlets.Lookup(r.Root)
}

func (r *Rule) RootView() *View {
	return r.RootViewlet().View
}

func (r *Rule) updateExclude(n *Viewlet) {
	prefix := ""
	r.updateViewExclude(n, prefix)
}

func (r *Rule) updateViewExclude(n *Viewlet, prefix string) {
	if n.Holder != "" {
		prefix += n.Holder + "."
	}
	fmt.Printf("updating exclude: %v %v\n", n.Name, n.Holder)

	for _, exclude := range n.View.Exclude { //Todo convert to field name
		field := n.Spec.Type.ByColumn(exclude)
		r.Route.Output.Exclude = append(r.Route.Output.Exclude, prefix+field.Name)
	}

	for _, rel := range n.View.With {
		viewName := strings.Replace(rel.Of.View.Name, "#", "", 1)
		relViewlet := r.Viewlets.Lookup(viewName)
		r.updateViewExclude(relViewlet, prefix)
	}
	n.View.Exclude = nil //TODO do we have to remove it
}

func (r *Rule) applyRootViewRouteShorthands() {
	root := r.RootViewlet()
	setter.SetStringIfEmpty(&r.Route.Output.Field, root.Field)
	if r.Route.Output.Style == "" {
		r.Route.Output.Style = contract.Style(root.Style)
	}
	if r.Route.Output.Cardinality == "" {
		r.Route.Output.Cardinality = root.ViewCardinality()
	}

}

func (r *Rule) applyShortHands() {
	if r.ResponseBody != nil {
		r.Route.Output.ResponseBody = &contract.BodySelector{}
		r.Route.Output.ResponseBody.StateValue = r.ResponseBody.From
	}
	if r.Type != "" {
		r.Handler = &handler.Handler{
			Type:       r.Type,
			InputType:  r.InputType,
			OutputType: r.OutputType,
			Arguments:  r.HandlerArgs,
		}

	}
	if r.Route.Output.Field != "" {
		r.Route.Output.Style = contract.ComprehensiveStyle
	}
	if r.Route.TabularJSON != nil && r.Route.Output.DataFormat == "" {
		r.Route.Output.DataFormat = content.JSONDataFormatTabular
	}
	if r.Route.XML != nil && r.Route.Output.DataFormat == "" {
		r.Route.Output.DataFormat = content.XMLFormat
	}
}

func (r *Rule) IsReader() bool {
	return r.Service == "" || r.Service == service.TypeReader
}

func NewRule() *Rule {
	return &Rule{Viewlets: Viewlets{registry: map[string]*Viewlet{}}, With: []string{
		"connections",
	}}
}
