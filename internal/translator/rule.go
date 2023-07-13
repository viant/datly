package translator

import (
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"strings"
)

type (
	Rule struct {
		Namespaces
		orderNamespaces []string
		Root            string
		router.Route

		Async        *AsyncConfig              `json:",omitempty"`
		ConstFileURL string                    `json:",omitempty"`
		Cache        *view.Cache               `json:",omitempty"`
		CSV          *router.CSVConfig         `json:",omitempty"`
		Const        map[string]interface{}    `json:",omitempty"`
		ConstURL     string                    `json:",omitempty"`
		Field        string                    `json:",omitempty"`
		RequestBody  *BodyConfig               `json:",omitempty"`
		TypeSrc      *TypeSrcConfig            `json:",omitempty"`
		ResponseBody *ResponseBodyConfig       `json:",omitempty"`
		Package      string                    `json:",omitempty"`
		Router       *RouterConfig             `json:",omitempty" yaml:",omitempty"`
		DataFormat   string                    `json:",omitempty"`
		TabularJSON  *router.TabularJSONConfig `json:",omitempty"`
		HandlerType  string                    `json:",omitempty"`
		StateType    string                    `json:",omitempty"`
		indexNamespaces
		With []string
	}

	indexNamespaces []*indexNamespace

	indexNamespace struct {
		Name      string
		Namespace string
	}

	TypeSrcConfig struct {
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

func (r *Resource) initRule() {
	rule := r.Rule
	r.State.AppendConstants(rule.Const)
	rule.Index = router.Index{Namespace: map[string]string{}}
	rule.applyDefaults()
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

func (r *Rule) RootView() *View {
	return r.Namespaces.Lookup(r.Root).View
}

func (r *Rule) updateExclude(n *Namespace) {
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

func NewRule() *Rule {
	return &Rule{Namespaces: Namespaces{registry: map[string]*Namespace{}}, With: []string{
		"connections",
	}}
}
