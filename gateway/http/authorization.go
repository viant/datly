package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/viant/authz"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xresponse "github.com/viant/xdatly/response"
)

// AuthorizationConfig opts all HTTP dispatch into a default authz policy.
// Overrides select policies for existing component route templates or exact paths.
// Exclusions are exact method/path pairs, never prefixes or templates.
type AuthorizationConfig struct {
	Provider      string                  `json:"Provider" yaml:"Provider"`
	DefaultPolicy string                  `json:"DefaultPolicy" yaml:"DefaultPolicy"`
	Policies      map[string]authz.Policy `json:"Policies" yaml:"Policies"`
	Overrides     []AuthorizationOverride `json:"Overrides,omitempty" yaml:"Overrides,omitempty"`
	Exclude       []spec.RouteRef         `json:"Exclude,omitempty" yaml:"Exclude,omitempty"`
}
type AuthorizationOverride struct {
	Match  spec.RouteRef `json:"Match" yaml:"Match"`
	Policy string        `json:"Policy" yaml:"Policy"`
}

// AuthorizationProvider adapts a request to verified facts. It may return a
// derived context carrying its existing identity scope for downstream reuse.
// Implementations must preserve cancellation and return ErrUnauthenticated for
// missing/invalid credentials. Unknown errors are dependency failures, not denials.
type AuthorizationProvider func(context.Context, *http.Request) (context.Context, authz.Facts, error)

var ErrUnauthenticated = errors.New("HTTP authentication required")

type authorizationFactsKey struct{}
type authorizationInvokerKey struct{}

// AuthorizationComponentInvoker supplies native component invocation to a trusted
// identity hook. It is bound to this request's generation and header/query inputs;
// it never dispatches HTTP or reads the endpoint's request body.
func AuthorizationComponentInvoker(ctx context.Context) dexec.ComponentInvoker {
	invoker, _ := ctx.Value(authorizationInvokerKey{}).(dexec.ComponentInvoker)
	return invoker
}

type authorizationComponentInvoker struct {
	owner     dexec.ComponentInvoker
	request   *http.Request
	once      sync.Once
	providers []locator.Provider
}

func (i *authorizationComponentInvoker) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	i.once.Do(func() {
		i.providers = requestprovider.NewValues(requestprovider.WithHeaders(i.request.Header), requestprovider.WithQuery(i.request.URL.Query())).Providers()
	})
	request.Providers = append(append([]locator.Provider(nil), i.providers...), request.Providers...)
	return i.owner.InvokeComponent(ctx, request)
}

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
	overrides     map[spec.RouteRef]string
	exclude       map[spec.RouteRef]bool
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
	g := &authorizationGate{provider: provider, policies: config.Policies, defaultPolicy: config.DefaultPolicy, overrides: map[spec.RouteRef]string{}, exclude: map[spec.RouteRef]bool{}}
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

func validAuthorizationMatch(ref spec.RouteRef, exact bool) error {
	u, err := url.ParseRequestURI(ref.Path)
	if err != nil || !strings.HasPrefix(ref.Path, "/") || strings.HasPrefix(ref.Path, "//") || u.RawQuery != "" || u.Fragment != "" || ref.Method == "" || strings.ToUpper(ref.Method) != ref.Method || strings.ContainsAny(ref.Method, " \t\r\n*") || (exact && strings.ContainsAny(ref.Path, "{}*")) {
		return fmt.Errorf("invalid authorization match %q %q", ref.Method, ref.Path)
	}
	return nil
}

func (h *Handler) authorizeHTTP(req *http.Request) (*http.Request, error) {
	g := h.authorization
	if g == nil {
		return req, nil
	}
	ref := spec.RouteRef{Method: req.Method, Path: h.routingPath(req)}
	if g.exclude[ref] {
		return req, nil
	}
	name := g.defaultPolicy
	// Literal bindings are exact. Parameterized bindings use the owning router.
	if selected := g.overrides[ref]; selected != "" {
		name = selected
	} else if route, ok := h.runtime.RouteByMethodPath(ref.Method, ref.Path); ok {
		if selected := g.overrides[spec.RouteRef{Method: route.Method, Path: route.Path}]; selected != "" {
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
	hookContext := context.WithValue(req.Context(), authorizationInvokerKey{}, &authorizationComponentInvoker{owner: h.runtime, request: req})
	ctx, facts, err := g.provider(hookContext, req.WithContext(hookContext))
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
		if code := xresponse.ErrorStatusCode(err, 0); code >= 400 && code <= 599 {
			return code
		}
		return http.StatusServiceUnavailable
	}
}
