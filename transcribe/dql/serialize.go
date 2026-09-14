package dql

import (
	"fmt"
	"github.com/viant/datly/spec"
	"strconv"
	"strings"
)

// SourceExport separates retained authorship from bounded reconstruction.
type SourceExport struct {
	Source      string   `json:"source"`
	Original    bool     `json:"original"`
	Supported   bool     `json:"supported"`
	Limitations []string `json:"limitations"`
}
type Serializer struct{}

func (Serializer) Export(component *spec.Component, authored string) SourceExport {
	if authored != "" {
		return SourceExport{Source: authored, Original: true, Supported: true, Limitations: []string{"retained source; recompilation requires its original package/type/resource authority"}}
	}
	result := SourceExport{Limitations: []string{}}
	if component == nil {
		result.Limitations = append(result.Limitations, "component metadata is absent")
		return result
	}
	if len(component.Routes) != 1 {
		result.Limitations = append(result.Limitations, "reconstruction requires exactly one route")
	}
	for _, route := range component.Routes {
		if route.Handler != "" {
			result.Limitations = append(result.Limitations, "compiled Go or handler behavior is opaque")
		}
		if route.CORS != nil || len(route.MCP) > 0 || route.APIKeyHeader != "" || route.Marshaller != "" {
			result.Limitations = append(result.Limitations, "route authorization, exposure or presentation metadata is not reconstructed")
		}
	}
	if len(component.Parameters) > 0 || len(component.Views) > 0 {
		result.Limitations = append(result.Limitations, "parameter and independent-view contracts require authored declarations")
	}
	if v := component.RootView; v == nil || v.Source == nil || v.Source.SQL == "" {
		result.Limitations = append(result.Limitations, "an explicit SQL root is required")
	} else if len(v.Relations) > 0 || v.Selector != nil || v.Partitioning != nil {
		result.Limitations = append(result.Limitations, "relation/selector/partition metadata is not reconstructed")
	}
	if s := component.Settings; s != nil {
		copy := s.Clone()
		copy.DefaultConnector = ""
		if copy.InputType != "" || copy.OutputType != "" || copy.Cache != nil || copy.Report != nil || copy.Generation != nil || len(copy.MCPFolders) > 0 || copy.Output != nil || len(copy.Const) > 0 || copy.Format != "" || copy.CaseFormat != "" || copy.DateFormat != "" || copy.JSONMarshalType != "" || copy.JSONUnmarshalType != "" || copy.XMLUnmarshalType != "" || copy.IgnoreEmptyQueryParameters != nil {
			result.Limitations = append(result.Limitations, "settings beyond the connector are not reconstructed")
		}
	}
	if len(result.Limitations) > 0 {
		return result
	}
	route := component.Routes[0]
	var text strings.Builder
	fmt.Fprintf(&text, "#setting($_ = $route(%s, %s))\n", strconv.Quote(route.Path), strconv.Quote(route.Method))
	if component.Settings != nil && component.Settings.DefaultConnector != "" {
		fmt.Fprintf(&text, "#setting($_ = $connector(%s))\n", strconv.Quote(component.Settings.DefaultConnector))
	}
	text.WriteString(component.RootView.Source.SQL)
	result.Source = text.String()
	result.Supported = true
	result.Limitations = append(result.Limitations, "reconstructed route/connector/root SQL only; not original bytes or opaque Go behavior")
	return result
}
