package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/viant/authz"
	"github.com/viant/datly/repository/contract"
)

// AuthorizationConfig opts all HTTP dispatch into a default authz policy.
// Overrides select policies for existing component route templates or exact paths.
// Exclusions are exact method/path pairs, never prefixes or templates.
type AuthorizationConfig struct {
	Provider      string                  `json:"Provider" yaml:"Provider"`
	DefaultPolicy string                  `json:"DefaultPolicy" yaml:"DefaultPolicy"`
	Policies      map[string]authz.Policy `json:"Policies" yaml:"Policies"`
	Overrides     []AuthorizationOverride `json:"Overrides,omitempty" yaml:"Overrides,omitempty"`
	Exclude       []AuthorizationMatch    `json:"Exclude,omitempty" yaml:"Exclude,omitempty"`
}
type AuthorizationOverride struct {
	Match  AuthorizationMatch `json:"Match" yaml:"Match"`
	Policy string             `json:"Policy" yaml:"Policy"`
}

// AuthorizationProvider adapts a request to verified facts. It may return a
// derived context carrying its existing identity scope for downstream reuse.
// Implementations must preserve cancellation and return ErrUnauthenticated for
// missing/invalid credentials. Unknown errors are dependency failures, not denials.
type AuthorizationProvider func(context.Context, *http.Request) (context.Context, authz.Facts, error)

var ErrUnauthenticated = errors.New("HTTP authentication required")

type authorizationFactsKey struct{}
type authorizationInvokerKey struct{}

// AuthorizationComponentInvoker exposes the existing native dispatcher to an
// identity hook. It binds request metadata without recursively routing HTTP.
func AuthorizationComponentInvoker(ctx context.Context) contract.Dispatcher {
	invoker, _ := ctx.Value(authorizationInvokerKey{}).(contract.Dispatcher)
	return invoker
}

type authorizationComponentInvoker struct {
	owner   contract.Dispatcher
	request *http.Request
}

func (i *authorizationComponentInvoker) Dispatch(ctx context.Context, path *contract.Path, options ...contract.Option) (interface{}, error) {
	request := i.request.Clone(ctx)
	// Identity binding must not consume the protected endpoint's body.
	request.Body = http.NoBody
	request.ContentLength = 0
	defaults := []contract.Option{contract.WithRequest(request), contract.WithHeader(request.Header), contract.WithQuery(request.URL.Query())}
	return i.owner.Dispatch(ctx, path, append(defaults, options...)...)
}

type AuthorizationMatch struct {
	Method string `json:"Method" yaml:"Method"`
	Path   string `json:"Path" yaml:"Path"`
}

func (m AuthorizationMatch) String() string { return m.Method + ":" + m.Path }

// AuthorizationFacts exposes the immutable verified snapshot for this request.
// It is not a resource authorization decision.
func AuthorizationFacts(ctx context.Context) *authz.PreparedFacts {
	facts, _ := ctx.Value(authorizationFactsKey{}).(*authz.PreparedFacts)
	return facts
}

type authorizationGate struct {
	provider      AuthorizationProvider
	policies      map[string]authz.Policy
	defaultPolicy string
	overrides     map[AuthorizationMatch]string
	exclude       map[AuthorizationMatch]bool
}

func (c Config) authorizationGate() (*authorizationGate, error) {
	if c.Authorization == nil {
		return nil, nil
	}
	// Detach caller-owned nested rule trees and slices at generation build time.
	raw, err := json.Marshal(c.Authorization)
	if err != nil {
		return nil, err
	}
	var config AuthorizationConfig
	if err = json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	provider := c.AuthorizationProviders[config.Provider]
	if config.Provider == "" || provider == nil {
		return nil, fmt.Errorf("Authorization.Provider %q is not registered", config.Provider)
	}
	if config.DefaultPolicy == "" {
		return nil, fmt.Errorf("Authorization.DefaultPolicy is required")
	}
	g := &authorizationGate{provider: provider, policies: config.Policies, defaultPolicy: config.DefaultPolicy, overrides: map[AuthorizationMatch]string{}, exclude: map[AuthorizationMatch]bool{}}
	for name, policy := range g.policies {
		if name == "" {
			return nil, fmt.Errorf("empty authorization policy name")
		}
		if policy.Named != nil || policy.Definition != nil || policy.EntityType != "" {
			return nil, fmt.Errorf("authorization policy %q must be a local, unbounded HTTP admission policy", name)
		}
		if err := authz.ValidatePolicy("execute", policy); err != nil {
			return nil, fmt.Errorf("authorization policy %q: %w", name, err)
		}
	}
	if _, ok := g.policies[g.defaultPolicy]; !ok {
		return nil, fmt.Errorf("unknown default policy %q", g.defaultPolicy)
	}
	for _, ref := range config.Exclude {
		if err := validAuthorizationMatch(ref, true); err != nil {
			return nil, err
		}
		if g.exclude[ref] {
			return nil, fmt.Errorf("duplicate authorization exclusion %s", ref.String())
		}
		g.exclude[ref] = true
	}
	for _, override := range config.Overrides {
		ref := override.Match
		if err := validAuthorizationMatch(ref, false); err != nil {
			return nil, err
		}
		if g.exclude[ref] || g.overrides[ref] != "" {
			return nil, fmt.Errorf("conflicting authorization match %s", ref.String())
		}
		if _, ok := g.policies[override.Policy]; !ok {
			return nil, fmt.Errorf("unknown authorization policy %q", override.Policy)
		}
		g.overrides[ref] = override.Policy
	}
	return g, nil
}

func validAuthorizationMatch(ref AuthorizationMatch, exact bool) error {
	u, err := url.ParseRequestURI(ref.Path)
	if err != nil || !strings.HasPrefix(ref.Path, "/") || strings.HasPrefix(ref.Path, "//") || u.RawQuery != "" || u.Fragment != "" || ref.Method == "" || strings.ToUpper(ref.Method) != ref.Method || strings.ContainsAny(ref.Method, " \t\r\n*") || (exact && strings.ContainsAny(ref.Path, "{}*")) {
		return fmt.Errorf("invalid authorization match %q %q", ref.Method, ref.Path)
	}
	return nil
}

func (h *Router) authorizeHTTP(req *http.Request, route *Route) (*http.Request, error) {
	g := h.authorization
	if g == nil {
		return req, nil
	}
	ref := AuthorizationMatch{Method: req.Method, Path: req.URL.EscapedPath()}
	if g.exclude[ref] {
		return req, nil
	}
	name := g.defaultPolicy
	// Literal bindings are exact. Parameterized bindings use the owning router.
	if selected := g.overrides[ref]; selected != "" {
		name = selected
	} else if route != nil && route.Path != nil {
		if selected := g.overrides[AuthorizationMatch{Method: route.Path.Method, Path: route.Path.URI}]; selected != "" {
			name = selected
		}
	}
	policy := g.policies[name]
	if err := req.Context().Err(); err != nil {
		return req, err
	}
	if policy.Mode == "deny" {
		return req, authz.ErrDenied
	}
	if policy.Mode == "public" {
		return req, nil
	}
	hookContext := context.WithValue(req.Context(), authorizationInvokerKey{}, &authorizationComponentInvoker{owner: h.authorizationDispatcher, request: req})
	ctx, facts, err := g.provider(hookContext, req.WithContext(hookContext))
	if cancellation := req.Context().Err(); cancellation != nil {
		return req, cancellation
	}
	if err != nil {
		return req, err
	}
	if ctx == nil {
		return req, fmt.Errorf("authorization provider returned no context")
	}
	if err = req.Context().Err(); err != nil {
		return req, err
	}
	if err = ctx.Err(); err != nil {
		return req, err
	}
	now := time.Now()
	if facts.Subject == "" || facts.Tenant == "" || facts.Issuer == "" || !facts.ValidUntil.After(now) {
		return req, ErrUnauthenticated
	}
	prepared := authz.PrepareFacts(facts)
	_, err = authz.EvaluatePrepared(authz.Request{Resource: authz.Resource{Kind: "http", ID: ref.String(), Tenant: facts.Tenant}, Action: name}, g.policies, prepared, now)
	if err != nil {
		return req, err
	}
	return req.WithContext(context.WithValue(ctx, authorizationFactsKey{}, prepared)), nil
}

func authorizationErrorStatus(err error) int {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout
	case errors.Is(err, ErrUnauthenticated), errors.Is(err, authz.ErrIdentityDenied):
		return http.StatusUnauthorized
	case errors.Is(err, authz.ErrUnavailable):
		return http.StatusServiceUnavailable
	case errors.Is(err, authz.ErrDenied):
		return http.StatusForbidden
	default:
		var status interface{ StatusCode() int }
		if errors.As(err, &status) && status.StatusCode() >= 400 && status.StatusCode() <= 599 {
			return status.StatusCode()
		}
		return http.StatusServiceUnavailable
	}
}

// Keep provider causes available to callers without returning sensitive messages.
type authorizationFailure struct {
	code  int
	cause error
}

func (e *authorizationFailure) Error() string   { return http.StatusText(e.code) }
func (e *authorizationFailure) Message() string { return e.Error() }
func (e *authorizationFailure) StatusCode() int { return e.code }
func (e *authorizationFailure) Unwrap() error   { return e.cause }
