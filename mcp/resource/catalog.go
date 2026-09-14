package resource

import (
	"fmt"
	"net/url"
	"sort"

	"github.com/viant/mcp-protocol/schema"
)

type Catalog struct {
	static    map[string]*Plan
	templates []*Plan
	names     []string
}

func NewCatalog(plans []*Plan) (*Catalog, error) {
	result := &Catalog{static: map[string]*Plan{}}
	names := map[string]bool{}
	uriTemplates := map[string]bool{}
	for _, plan := range plans {
		if plan == nil || plan.URI() == "" {
			return nil, fmt.Errorf("MCP resource plan and URI are required")
		}
		name := planName(plan)
		if names[name] {
			return nil, fmt.Errorf("duplicate MCP resource name %q", name)
		}
		names[name] = true
		result.names = append(result.names, name)
		if plan.resource != nil {
			if _, ok := result.static[plan.resource.Uri]; ok {
				return nil, fmt.Errorf("duplicate MCP resource URI %q", plan.resource.Uri)
			}
			result.static[plan.resource.Uri] = plan
			continue
		}
		if plan.template == nil {
			return nil, fmt.Errorf("MCP resource plan %q has no metadata", name)
		}
		if uriTemplates[plan.template.UriTemplate] {
			return nil, fmt.Errorf("duplicate MCP resource template URI %q", plan.template.UriTemplate)
		}
		uriTemplates[plan.template.UriTemplate] = true
		result.templates = append(result.templates, plan)
	}
	sort.Strings(result.names)
	for uri, file := range result.static {
		if file.file == nil {
			continue
		}
		parsed, _ := url.Parse(uri)
		for _, template := range result.templates {
			if parsed.Scheme == template.scheme && parsed.Host == template.authority {
				if _, matches, err := template.path.MatchEscapedPath(parsed.EscapedPath()); err != nil || matches {
					return nil, fmt.Errorf("published file %q overlaps a component resource template", uri)
				}
			}
		}
	}
	sort.SliceStable(result.templates, func(i, j int) bool { return result.templates[i].URI() < result.templates[j].URI() })
	return result, nil
}

func (c *Catalog) Names() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.names...)
}

func (c *Catalog) Resolve(rawURI string) (*Resolved, error) {
	if c == nil {
		return nil, fmt.Errorf("MCP resource catalog is required")
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURI, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: scheme and authority are required without user info or fragment", ErrInvalidURI)
	}
	if plan := c.static[rawURI]; plan != nil {
		if plan.file != nil {
			return &Resolved{plan: plan, uri: rawURI}, nil
		}
		scope, scopeErr := plan.scope(nil, nil)
		if scopeErr != nil {
			return nil, scopeErr
		}
		return &Resolved{plan: plan, scope: scope, uri: rawURI}, nil
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURI, err)
	}
	var matched *Resolved
	var candidateErr error
	for _, plan := range c.templates {
		if parsed.Scheme != plan.scheme || parsed.Host != plan.authority {
			continue
		}
		path, ok, matchErr := plan.path.MatchEscapedPath(parsed.EscapedPath())
		if matchErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidURI, matchErr)
		}
		if !ok {
			continue
		}
		scope, scopeErr := plan.scope(path, query)
		if scopeErr != nil {
			if candidateErr == nil {
				candidateErr = fmt.Errorf("%w: %v", ErrInvalidURI, scopeErr)
			}
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("%w: %q matches multiple templates", ErrAmbiguous, rawURI)
		}
		matched = &Resolved{plan: plan, scope: scope, uri: rawURI}
	}
	if matched == nil {
		if candidateErr != nil {
			return nil, candidateErr
		}
		return nil, fmt.Errorf("%w: %q", ErrNotFound, rawURI)
	}
	return matched, nil
}

func (c *Catalog) Resources() []schema.Resource {
	if c == nil {
		return nil
	}
	result := make([]schema.Resource, 0, len(c.static))
	for _, plan := range c.static {
		metadata, _ := plan.Resource()
		result = append(result, metadata)
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Uri < result[j].Uri })
	return result
}

func (c *Catalog) Templates() []schema.ResourceTemplate {
	if c == nil {
		return nil
	}
	result := make([]schema.ResourceTemplate, 0, len(c.templates))
	for _, plan := range c.templates {
		metadata, _ := plan.Template()
		result = append(result, metadata)
	}
	return result
}

func planName(plan *Plan) string {
	if plan.resource != nil {
		return plan.resource.Name
	}
	if plan.template != nil {
		return plan.template.Name
	}
	return ""
}
