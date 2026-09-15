package config

import (
	"fmt"

	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/spec"
	"github.com/viant/scy"
)

// ResolveConstants stages only fields whose owners access paths or URLs. It
// retains the caller's configuration and root handles without widening authority.
// The returned configuration is a server-lifetime access plan, never persisted.
func (c *Config) ResolveConstants() (*Config, error) {
	if c == nil {
		return nil, fmt.Errorf("standalone configuration is required")
	}
	result := *c
	if c.Const == nil {
		return &result, nil
	}
	paths := []*string{&result.BaseDir, &result.ContentURL, &result.RouteURL, &result.PluginsURL, &result.DependencyURL, &result.JobURL, &result.FailedJobURL}
	result.ModuleDirs = append([]string(nil), c.ModuleDirs...)
	for i := range result.ModuleDirs {
		paths = append(paths, &result.ModuleDirs[i])
	}
	result.Connectors = append([]connector.Config(nil), c.Connectors...)
	secrets := map[*scy.Resource]*scy.Resource{}
	var cloneSecret func(*scy.Resource) *scy.Resource
	cloneSecret = func(source *scy.Resource) *scy.Resource {
		if source == nil {
			return nil
		}
		if existing := secrets[source]; existing != nil {
			return existing
		}
		result := *source
		secrets[source] = &result
		result.Data = append([]byte(nil), source.Data...)
		result.Options = append(source.Options[:0:0], source.Options...)
		paths = append(paths, &result.URL)
		result.Fallback = cloneSecret(source.Fallback)
		return &result
	}
	for i := range result.Connectors {
		paths = append(paths, &result.Connectors[i].DSN)
		result.Connectors[i].Secret = cloneSecret(c.Connectors[i].Secret)
	}
	result.StaticContent = make([]*spec.StaticContent, len(c.StaticContent))
	for i, item := range c.StaticContent {
		result.StaticContent[i] = item.Clone()
		if item != nil {
			paths = append(paths, &result.StaticContent[i].ContentURL)
		}
	}
	if c.Jobs != nil {
		jobs := *c.Jobs
		result.Jobs = &jobs
		paths = append(paths, &jobs.Notification.Destination)
	}
	if c.JWTValidator != nil {
		jwt := *c.JWTValidator
		jwt.RSA = append(jwt.RSA[:0:0], c.JWTValidator.RSA...)
		for i := range jwt.RSA {
			jwt.RSA[i] = cloneSecret(jwt.RSA[i])
		}
		jwt.HMAC = cloneSecret(jwt.HMAC)
		jwt.Rules = append(jwt.Rules[:0:0], c.JWTValidator.Rules...)
		for i, source := range jwt.Rules {
			if source == nil {
				continue
			}
			rule := *source
			rule.Resource = append([]string(nil), source.Resource...)
			rule.RSA = append(rule.RSA[:0:0], source.RSA...)
			for j := range rule.RSA {
				rule.RSA[j] = cloneSecret(rule.RSA[j])
			}
			rule.HMAC = cloneSecret(rule.HMAC)
			jwt.Rules[i] = &rule
		}
		result.JWTValidator = &jwt
		paths = append(paths, &jwt.CertURL)
	}
	if c.MCP != nil {
		mcp := *c.MCP
		result.MCP = &mcp
		mcp.Folders = append(mcp.Folders[:0:0], c.MCP.Folders...)
		for i := range mcp.Folders {
			paths = append(paths, &mcp.Folders[i].Root, &mcp.Folders[i].LocalDir, &mcp.Folders[i].URIPrefix)
		}
	}
	if c.Observation != nil && c.Observation.OTel != nil {
		observation := *c.Observation
		telemetry := *observation.OTel
		observation.OTel = &telemetry
		result.Observation = &observation
		paths = append(paths, &telemetry.HTTP.EndpointURL)
	}
	if c.OpenAPI != nil {
		openapi := *c.OpenAPI
		result.OpenAPI = &openapi
		openapi.StartupExports = append(openapi.StartupExports[:0:0], c.OpenAPI.StartupExports...)
		for i := range openapi.StartupExports {
			paths = append(paths, &openapi.StartupExports[i].URL)
		}
	}
	for _, path := range paths {
		resolved, err := c.Const.Path(*path)
		if err != nil {
			return nil, fmt.Errorf("configured resource path: %w", err)
		}
		*path = resolved
	}
	if err := (Loader{StaticLocalRoot: c.StaticLocalRoot}).normalize(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
