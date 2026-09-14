// Package resource compiles and resolves MCP resource route exposures.
package resource

import (
	"fmt"
	"maps"
	"net/url"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/runtime/registry"
	runtimeRoute "github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	skillformat "github.com/viant/mcp-protocol/extension/skills"
	"github.com/viant/mcp-protocol/schema"
)

type Plan struct {
	skill     *skillformat.Static
	file      *fileResource
	kind      spec.MCPExposureKind
	resource  *schema.Resource
	template  *schema.ResourceTemplate
	target    exec.ComponentTarget
	input     *registry.RouteInputContract
	path      *runtimeRoute.PathTemplate
	binding   *mcpinput.Plan
	scheme    string
	authority string
}

func (p *Plan) Kind() spec.MCPExposureKind {
	if p == nil {
		return ""
	}
	return p.kind
}

func (p *Plan) Resource() (schema.Resource, bool) {
	if p == nil || p.resource == nil {
		return schema.Resource{}, false
	}
	return cloneResource(*p.resource), true
}

func (p *Plan) Template() (schema.ResourceTemplate, bool) {
	if p == nil || p.template == nil {
		return schema.ResourceTemplate{}, false
	}
	return cloneTemplate(*p.template), true
}

func (p *Plan) Target() exec.ComponentTarget {
	if p == nil {
		return exec.ComponentTarget{}
	}
	return p.target
}

func (p *Plan) Input() *registry.RouteInputContract {
	if p == nil {
		return nil
	}
	return p.input
}

func (p *Plan) URI() string {
	if p == nil {
		return ""
	}
	if p.resource != nil {
		return p.resource.Uri
	}
	if p.template != nil {
		return p.template.UriTemplate
	}
	return ""
}

func (p *Plan) MIMEType() string {
	if p == nil {
		return ""
	}
	if p.resource != nil && p.resource.MimeType != nil {
		return *p.resource.MimeType
	}
	if p.template != nil && p.template.MimeType != nil {
		return *p.template.MimeType
	}
	return ""
}

type Resolved struct {
	plan  *Plan
	scope *requestprovider.Scope
	uri   string
}

func (r *Resolved) Plan() *Plan {
	if r == nil {
		return nil
	}
	return r.plan
}

func (r *Resolved) Scope() *requestprovider.Scope {
	if r == nil {
		return nil
	}
	return r.scope
}

func (r *Resolved) URI() string {
	if r == nil {
		return ""
	}
	return r.uri
}

func (p *Plan) scope(path map[string]string, query url.Values) (*requestprovider.Scope, error) {
	if p == nil || p.binding == nil {
		return nil, fmt.Errorf("MCP resource binding plan is unavailable")
	}
	return p.binding.Scope(mcpinput.URI{Path: path, Query: query})
}

func cloneResource(source schema.Resource) schema.Resource {
	result := source
	result.Description = cloneString(source.Description)
	result.MimeType = cloneString(source.MimeType)
	result.Title = cloneString(source.Title)
	result.Icons = append([]schema.Icon(nil), source.Icons...)
	if source.Size != nil {
		size := *source.Size
		result.Size = &size
	}
	result.Meta = maps.Clone(source.Meta)
	return result
}

func cloneTemplate(source schema.ResourceTemplate) schema.ResourceTemplate {
	result := source
	result.Description = cloneString(source.Description)
	result.MimeType = cloneString(source.MimeType)
	result.Title = cloneString(source.Title)
	result.Icons = append([]schema.Icon(nil), source.Icons...)
	return result
}

func cloneString(source *string) *string {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}
