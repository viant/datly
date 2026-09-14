package spec

// Clone returns an isolated component metadata graph.
func (c *Component) Clone() *Component {
	if c == nil {
		return nil
	}
	result := *c
	result.Documentation = c.Documentation.Clone()
	result.Static = c.Static.Clone()
	result.Settings = c.Settings.Clone()
	result.TypeContext = c.TypeContext.Clone()
	clonedViews := map[*View]*View{}
	result.RootView = cloneView(c.RootView, clonedViews)
	result.Views = make([]*View, len(c.Views))
	for index, view := range c.Views {
		result.Views[index] = cloneView(view, clonedViews)
	}
	result.Routes = make([]*Route, len(c.Routes))
	for i, route := range c.Routes {
		result.Routes[i] = route.Clone()
	}
	result.Parameters = make([]*Parameter, len(c.Parameters))
	for i, param := range c.Parameters {
		result.Parameters[i] = param.Clone()
	}
	return &result
}

func (s *Settings) Clone() *Settings {
	if s == nil {
		return nil
	}
	result := *s
	result.MCPFolders = append([]ResourceFolder(nil), s.MCPFolders...)
	for i := range result.MCPFolders {
		result.MCPFolders[i] = result.MCPFolders[i].Clone()
	}
	if s.IgnoreEmptyQueryParameters != nil {
		value := *s.IgnoreEmptyQueryParameters
		result.IgnoreEmptyQueryParameters = &value
	}
	result.Generation = s.Generation.Clone()
	if s.Output != nil {
		value := *s.Output
		value.Exclude = append([]string(nil), s.Output.Exclude...)
		result.Output = &value
	}
	if s.Const != nil {
		result.Const = make(map[string]string, len(s.Const))
		for key, value := range s.Const {
			result.Const[key] = value
		}
	}
	result.Report = s.Report.Clone()
	if s.Cache != nil {
		cache := *s.Cache
		cache.Warmup = s.Cache.Warmup.Clone()
		result.Cache = &cache
	}
	return &result
}

func (s *ReportSettings) Clone() *ReportSettings {
	if s == nil {
		return nil
	}
	result := *s
	result.InputLayout = s.InputLayout.Clone()
	result.Compose = s.Compose.Clone()
	if s.MCPTool != nil {
		enabled := *s.MCPTool
		result.MCPTool = &enabled
	}
	return &result
}

func (s *ReportInputLayout) Clone() *ReportInputLayout {
	if s == nil {
		return nil
	}
	result := *s
	return &result
}

func (s *GenerationSettings) Clone() *GenerationSettings {
	if s == nil {
		return nil
	}
	result := *s
	return &result
}

func (r *Route) Clone() *Route {
	if r == nil {
		return nil
	}
	result := *r
	result.CORS = r.CORS.Resolve(nil)
	result.MCP = make([]*MCPExposure, len(r.MCP))
	for index, exposure := range r.MCP {
		result.MCP[index] = exposure.Clone()
	}
	return &result
}

func (c *TypeContext) Clone() *TypeContext {
	if c == nil {
		return nil
	}
	result := *c
	result.Imports = append([]ImportSpec(nil), c.Imports...)
	return &result
}

func (p *Parameter) Clone() *Parameter {
	if p == nil {
		return nil
	}
	result := *p
	for _, limit := range []**int{&result.MinAllowedRecords, &result.MaxAllowedRecords, &result.ExpectedReturned} {
		if *limit != nil {
			value := **limit
			*limit = &value
		}
	}
	if p.Required != nil {
		required := *p.Required
		result.Required = &required
	}
	if p.Cacheable != nil {
		cacheable := *p.Cacheable
		result.Cacheable = &cacheable
	}
	if p.Value != nil {
		value := *p.Value
		result.Value = &value
	}
	if p.Activation != nil {
		activation := *p.Activation
		result.Activation = &activation
	}
	if p.MCP != nil {
		value := *p.MCP
		result.MCP = &value
	}
	if p.PathMCP != nil {
		value := *p.PathMCP
		result.PathMCP = &value
	}
	if p.QuerySelector != nil {
		selector := *p.QuerySelector
		result.QuerySelector = &selector
	}
	result.Predicates = make([]*Predicate, len(p.Predicates))
	for i, predicate := range p.Predicates {
		if predicate != nil {
			item := *predicate
			item.Args = append([]string(nil), predicate.Args...)
			result.Predicates[i] = &item
		}
	}
	if p.Codec != nil {
		codec := *p.Codec
		codec.Args = append([]string(nil), p.Codec.Args...)
		result.Codec = &codec
	}
	return &result
}
