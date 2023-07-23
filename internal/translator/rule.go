package translator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type (
	Rule struct {
		Viewlets

		orderNamespaces []string
		Root            string
		router.Route
		Async        *AsyncConfig              `json:",omitempty"`
		Cache        *view.Cache               `json:",omitempty"`
		CSV          *router.CSVConfig         `json:",omitempty"`
		Const        map[string]interface{}    `json:",omitempty"`
		ConstURL     string                    `json:",omitempty"`
		Field        string                    `json:",omitempty"`
		RequestBody  *BodyConfig               `json:",omitempty"`
		TypeSrc      *parser.TypeImport        `json:",omitempty"`
		ResponseBody *ResponseBodyConfig       `json:",omitempty"`
		Package      string                    `json:",omitempty"`
		Router       *RouterConfig             `json:",omitempty" yaml:",omitempty"`
		DataFormat   string                    `json:",omitempty"`
		TabularJSON  *router.TabularJSONConfig `json:",omitempty"`
		HandlerType  string                    `json:",omitempty"`
		StateType    string                    `json:",omitempty"`
		With         []string                  `json:",omitempty"`
		indexNamespaces
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

	BodyConfig struct {
		DataType string `json:",omitempty"`
	}

	ResponseBodyConfig struct {
		From string
	}

	AsyncConfig struct {
		PrincipalSubject string `json:",omitempty" yaml:",omitempty"`
		Connector        string `json:",omitempty" yaml:",omitempty"`
		EnsureTable      *bool  `json:",omitempty" yaml:",omitempty"`
		ExpiryTimeInS    int    `json:",omitempty" yaml:",omitempty"`
		MarshalRelations *bool  `json:",omitempty" yaml:",omitempty"`
		Dataset          string `json:",omitempty" yaml:",omitempty"`
		BucketURL        string `json:",omitempty" yaml:",omitempty"`
	}
)

func (r *Rule) StateTypePackage() string {
	if r.StateType == "" {
		return ""
	}
	index := strings.LastIndex(r.StateType, ".")
	if index == -1 {
		return ""
	}
	return r.StateType[:index]
}

func (r *Rule) applyGeneratorOutputSetting() {
	root := r.RootViewlet()
	outputConfig := root.OutputSettings
	setter.SetStringIfEmpty(&r.Route.Field, outputConfig.Field)
	if r.Route.Style == "" && r.Route.Field != "" {
		r.Route.Style = router.ComprehensiveStyle
	}
	if r.Route.Style == "" {
		r.Route.Style = router.Style(outputConfig.Style)
	}
	if r.Route.Cardinality == "" {
		r.Route.Cardinality = outputConfig.ViewCardinality()
	}

}
func (r *Rule) DSQLSetting() interface{} {

	return struct {
		URI          string
		Method       string
		ResponseBody *ResponseBodyConfig `json:",omitempty"`
		HandlerType  string              `json:",omitempty"`
		StateType    string              `json:",omitempty"`
	}{
		URI:          r.URI,
		Method:       r.Method,
		ResponseBody: r.ResponseBody,
		HandlerType:  r.HandlerType,
		StateType:    r.StateType,
	}
}

func (r *Rule) ShallGenerateHandler() bool {
	return r.HandlerType != ""
}

func (r *Rule) IsMany() bool {
	return r.Cardinality == "" || r.Cardinality == view.Many
}

func (r *Rule) IsBasic() bool {
	return r.Style != router.ComprehensiveStyle && r.Field == ""
}

func (r *Rule) ExtractSettings(dSQL *string) error {
	if index := strings.Index(*dSQL, "*/"); index != -1 {
		if err := inference.TryUnmarshalHint((*dSQL)[:index+2], &r); err != nil {
			return err
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
		return "Data"
	}

	return r.Field
}

func (r *Resource) initRule(ctx context.Context, fs afs.Service) error {
	rule := r.Rule
	rule.Index = router.Index{Namespace: map[string]string{}}
	rule.applyDefaults()
	if err := r.loadConstants(ctx, fs, rule); err != nil {
		r.messages.AddWarning(r.rule.RuleName(), "const", fmt.Sprintf("failed to load constant : %v %w", rule.ConstURL, err))
	}
	r.State.AppendConstants(rule.Const)
	return nil
}

func (r *Resource) loadConstants(ctx context.Context, fs afs.Service, rule *Rule) error {
	constFileURL, err := r.getConstantURL(ctx, rule, fs)
	if err != nil || constFileURL == "" {
		return err
	}
	data, err := fs.DownloadWithURL(ctx, constFileURL)
	ext := path.Ext(constFileURL)
	switch ext {
	case ".yaml":
		if err = yaml.Unmarshal(data, &rule.Const); err != nil {
			return err
		}
	default:
		if err = json.Unmarshal(data, &rule.Const); err != nil {
			return err
		}
	}
	return nil
}

func (r *Resource) getConstantURL(ctx context.Context, rule *Rule, fs afs.Service) (string, error) {
	if rule.ConstURL == "" {
		return "", nil
	}
	if !url.IsRelative(rule.ConstURL) {
		return rule.ConstURL, nil
	}
	wd, _ := os.Getwd()
	constFileURL := filepath.Join(wd, rule.ConstURL)
	if ok, _ := fs.Exists(ctx, constFileURL); ok {
		return filepath.Join(wd, rule.ConstURL), nil
	}
	constFileURL = filepath.Join(r.rule.SourceDirectory(), rule.ConstURL)
	if ok, _ := fs.Exists(ctx, constFileURL); ok {
		return constFileURL, nil
	}
	return filepath.Join(r.rule.BaseRuleURL(), rule.ConstURL), nil
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
	setter.SetCaseFormatIfEmpty(&r.Output.CaseFormat, "lc")
	setter.SetBoolIfFalse(&r.EnableAudit, true)
	setter.SetBoolIfFalse(&r.CustomValidation, r.CustomValidation || r.HandlerType != "")
	if r.Route.Cors == nil {
		r.Route.Cors = &router.Cors{
			AllowCredentials: setter.BoolPtr(true),
			AllowHeaders:     setter.StringsPtr("*"),
			AllowMethods:     setter.StringsPtr("*"),
			AllowOrigins:     setter.StringsPtr("*"),
			ExposeHeaders:    setter.StringsPtr("*"),
		}
	}
}

func (r *Rule) RootViewlet() *Viewlet {
	return r.Viewlets.Lookup(r.Root)
}

func (r *Rule) RootView() *View {
	return r.RootViewlet().View
}

func (r *Rule) updateExclude(n *Viewlet) {
	if len(n.Exclude) == 0 {
		return
	}
	prefix := ""
	if n.Holder != "" {
		prefix = n.Holder + "."
	}
	for _, exclude := range n.View.Exclude { //Todo convert to field name
		field := n.Spec.Type.ByColumn(exclude)
		r.Route.Exclude = append(r.Route.Exclude, prefix+field.Name)
	}
	n.View.Exclude = nil //TODO do we have to remove it
}

func (r *Rule) applyRootViewRouteShorthands() {
	root := r.RootViewlet()
	setter.SetStringIfEmpty(&r.Route.Field, root.Field)
	if r.Route.Style == "" {
		r.Route.Style = router.Style(root.Style)
	}
	if r.Route.Cardinality == "" {
		r.Route.Cardinality = root.ViewCardinality()
	}

}

func (r *Rule) applyShortHands() {
	if r.ResponseBody != nil {
		r.Route.ResponseBody = &router.BodySelector{}
		r.Route.ResponseBody.StateValue = r.ResponseBody.From
	}
	if r.HandlerType != "" {
		r.Handler = &router.Handler{
			HandlerType: r.HandlerType,
			StateType:   r.StateType,
		}
	}
}

func NewRule() *Rule {
	return &Rule{Viewlets: Viewlets{registry: map[string]*Viewlet{}}, With: []string{
		"connections",
	}}
}
