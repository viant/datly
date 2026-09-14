package spec

// CORS preserves authored absence independently of explicit false/empty values.
// A nil route policy inherits the global policy; no policy enables nothing.
type CORS struct {
	AllowOrigins     *[]string `json:"allowOrigins,omitempty" yaml:"AllowOrigins,omitempty"`
	AllowMethods     *[]string `json:"allowMethods,omitempty" yaml:"AllowMethods,omitempty"`
	AllowHeaders     *[]string `json:"allowHeaders,omitempty" yaml:"AllowHeaders,omitempty"`
	ExposeHeaders    *[]string `json:"exposeHeaders,omitempty" yaml:"ExposeHeaders,omitempty"`
	AllowCredentials *bool     `json:"allowCredentials,omitempty" yaml:"AllowCredentials,omitempty"`
	MaxAge           *int64    `json:"maxAge,omitempty" yaml:"MaxAge,omitempty"`
}

// Resolve returns a detached policy with absent fields inherited from parent.
func (c *CORS) Resolve(parent *CORS) *CORS {
	if c == nil && parent == nil {
		return nil
	}
	result := &CORS{}
	if c != nil {
		*result = *c
	}
	if parent != nil {
		if result.AllowOrigins == nil {
			result.AllowOrigins = parent.AllowOrigins
		}
		if result.AllowMethods == nil {
			result.AllowMethods = parent.AllowMethods
		}
		if result.AllowHeaders == nil {
			result.AllowHeaders = parent.AllowHeaders
		}
		if result.ExposeHeaders == nil {
			result.ExposeHeaders = parent.ExposeHeaders
		}
		if result.AllowCredentials == nil {
			result.AllowCredentials = parent.AllowCredentials
		}
		if result.MaxAge == nil {
			result.MaxAge = parent.MaxAge
		}
		// Original Cors.Inherit restricts credentialed wildcard routes to the
		// configured parent origins instead of broadening that policy.
		if result.AllowCredentials != nil && *result.AllowCredentials && result.AllowOrigins != nil && len(*result.AllowOrigins) == 1 && (*result.AllowOrigins)[0] == "*" {
			result.AllowOrigins = parent.AllowOrigins
		}
	}
	for _, field := range []**[]string{&result.AllowOrigins, &result.AllowMethods, &result.AllowHeaders, &result.ExposeHeaders} {
		if *field != nil {
			values := append([]string{}, (**field)...)
			*field = &values
		}
	}
	if result.AllowCredentials != nil {
		value := *result.AllowCredentials
		result.AllowCredentials = &value
	}
	if result.MaxAge != nil {
		value := *result.MaxAge
		result.MaxAge = &value
	}
	return result
}

// DefaultCORS returns an independent original Datly default policy. Callers
// explicitly disable it with HTTP DisableCors or override fields with pointers.
func DefaultCORS() *CORS {
	yes := true
	origins, methods, headers, exposed := []string{"*"}, []string{"*"}, []string{"*"}, []string{"*"}
	return &CORS{AllowOrigins: &origins, AllowMethods: &methods, AllowHeaders: &headers, ExposeHeaders: &exposed, AllowCredentials: &yes}
}
