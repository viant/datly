package http

import (
	"fmt"
	"github.com/viant/datly/spec"
	stdhttp "net/http"
	"strconv"
	"strings"
)

type corsPolicy struct{ config *spec.CORS }

func newCORSPolicy(config *spec.CORS) (*corsPolicy, error) {
	if config == nil {
		return nil, nil
	}
	if config.AllowCredentials != nil && *config.AllowCredentials && config.AllowOrigins != nil {
		for _, origin := range *config.AllowOrigins {
			if origin == "*" {
				return nil, fmt.Errorf("credentialed CORS requires explicitly configured origins; wildcard is not allowed")
			}
		}
	}
	if config.MaxAge != nil && *config.MaxAge < 0 {
		return nil, fmt.Errorf("MaxAge must not be negative")
	}
	for _, values := range []*[]string{config.AllowOrigins, config.AllowMethods, config.AllowHeaders, config.ExposeHeaders} {
		if values == nil {
			continue
		}
		for _, value := range *values {
			if value == "" || strings.ContainsAny(value, "\r\n,") {
				return nil, fmt.Errorf("invalid CORS value %q", value)
			}
		}
	}
	return &corsPolicy{config: config}, nil
}

func (p *corsPolicy) allows(values *[]string, value string, fold bool) bool {
	if values == nil {
		return false
	}
	for _, allowed := range *values {
		if allowed == "*" || allowed == value || (fold && strings.EqualFold(allowed, value)) {
			return true
		}
	}
	return false
}

func (p *corsPolicy) apply(writer stdhttp.ResponseWriter, req *stdhttp.Request, method string, preflight bool) bool {
	if p == nil {
		return false
	}
	headers := writer.Header()
	p.vary(headers, "Origin")
	if preflight {
		headers.Add("Vary", "Access-Control-Request-Method")
		headers.Add("Vary", "Access-Control-Request-Headers")
	}
	origin := req.Header.Get("Origin")
	if origin == "" || !p.allows(p.config.AllowOrigins, origin, false) {
		return false
	}
	// Missing method policy permits the matched route only. Explicit empty denies.
	if p.config.AllowMethods != nil && !p.allows(p.config.AllowMethods, method, false) {
		return false
	}
	var requested []string
	if preflight {
		for _, name := range strings.Split(req.Header.Get("Access-Control-Request-Headers"), ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			if !p.allows(p.config.AllowHeaders, name, true) {
				return false
			}
			requested = append(requested, name)
		}
	}
	// Credentialed policies have been validated to contain explicit origins.
	headers.Set("Access-Control-Allow-Origin", origin)
	if p.config.AllowCredentials != nil && *p.config.AllowCredentials {
		headers.Set("Access-Control-Allow-Credentials", "true")
	}
	if preflight {
		headers.Set("Access-Control-Allow-Methods", method)
		if len(requested) > 0 {
			headers.Set("Access-Control-Allow-Headers", strings.Join(requested, ", "))
		}
		if p.config.MaxAge != nil {
			headers.Set("Access-Control-Max-Age", strconv.FormatInt(*p.config.MaxAge, 10))
		}
	} else if p.config.ExposeHeaders != nil {
		exposed := *p.config.ExposeHeaders
		// Original Datly expands this wildcard to these two explicit names. Keep
		// the browser-valid meaning even when credentials are enabled.
		if len(exposed) == 1 && exposed[0] == "*" {
			exposed = []string{"Content-Type", "Authorization"}
		}
		if len(exposed) > 0 {
			headers.Set("Access-Control-Expose-Headers", strings.Join(exposed, ", "))
		}
	}
	return true
}

func (h *Handler) serveCORS(writer stdhttp.ResponseWriter, req *stdhttp.Request) (stdhttp.ResponseWriter, bool) {
	method := req.Method
	preflight := method == stdhttp.MethodOptions && req.Header.Get("Origin") != "" && req.Header.Get("Access-Control-Request-Method") != ""
	if preflight {
		method = req.Header.Get("Access-Control-Request-Method")
	}
	if req.Method == stdhttp.MethodOptions && !preflight {
		if _, explicit := h.runtime.RouteByMethodPath(method, req.URL.EscapedPath()); !explicit {
			var allowed []string
			for _, actual := range h.runtime.AllowedMethodsForPath(req.URL.EscapedPath()) {
				endpoint, _ := h.runtime.RouteByMethodPath(actual, req.URL.EscapedPath())
				policy := h.cors[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()]
				if policy != nil && (policy.config.AllowMethods == nil || policy.allows(policy.config.AllowMethods, actual, false)) {
					allowed = append(allowed, actual)
				}
			}
			if len(allowed) > 0 {
				writer.Header().Set("Allow", strings.Join(append(allowed, "OPTIONS"), ", "))
				writer.WriteHeader(stdhttp.StatusNoContent)
				return writer, true
			}
		}
	}
	endpoint, ok := h.runtime.RouteByMethodPath(method, req.URL.EscapedPath())
	if !ok {
		return writer, false
	}
	policy := h.cors[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()]
	if policy == nil {
		return writer, false
	}
	allowed := policy.apply(writer, req, method, preflight)
	if preflight {
		if !allowed {
			writer.WriteHeader(stdhttp.StatusForbidden)
		} else {
			writer.WriteHeader(stdhttp.StatusNoContent)
		}
		return writer, true
	}
	return (&corsResponseWriter{ResponseWriter: writer, policy: policy, request: req, method: method}).wrap(), false
}

func (p *corsPolicy) vary(headers stdhttp.Header, name string) {
	for _, line := range headers.Values("Vary") {
		for _, token := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(token), name) || strings.TrimSpace(token) == "*" {
				return
			}
		}
	}
	headers.Add("Vary", name)
}
