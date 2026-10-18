package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/server/auth"
	"github.com/viant/scy/auth/flow"
	"golang.org/x/oauth2"
)

type switchingBFFSource struct {
	current atomic.Pointer[transportTestService]
}

func (s *switchingBFFSource) Pin(ctx context.Context) (context.Context, ServerService, error) {
	return ctx, s.current.Load(), nil
}

func TestSourceBFFChallengeExchangeRetainsBrokerState(t *testing.T) {
	var exchanges atomic.Int32
	idToken := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":4102444800}`)) + "."
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "test-client" || pass != "test-secret" {
			http.Error(w, "client authentication missing", http.StatusUnauthorized)
			return
		}
		if r.FormValue("code_verifier") == "" || r.FormValue("code") != "test-code" {
			http.Error(w, "missing PKCE exchange", http.StatusBadRequest)
			return
		}
		exchanges.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"access-from-provider","token_type":"Bearer","expires_in":3600,"id_token":"`+idToken+`"}`)
	}))
	defer identity.Close()
	policy := testAuthorizationPolicy()
	policy.Resources[testResourceURI].UseIdToken = true
	policy.Resources[testResourceURI].ProtectedResourceMetadata.AuthorizationServers = []string{identity.URL}
	service := newTransportTestService(policy)
	source := &switchingBFFSource{}
	source.current.Store(service)
	wire := startWireServerConfig(t, Config{
		Source: source,
		BackendForFrontend: &auth.BackendForFrontend{Client: &oauth2.Config{
			ClientID: "test-client", ClientSecret: "test-secret",
			Endpoint: oauth2.Endpoint{AuthURL: identity.URL + "/authorize", TokenURL: identity.URL + "/token", AuthStyle: oauth2.AuthStyleInHeader},
		}, AuthorizationExchangeHeader: flow.AuthorizationExchangeHeader},
		Transport: TransportConfig{Kind: TransportStreamable},
	})
	session := initializeWireSession(t, wire.baseURL)
	payload := map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": schema.MethodResourcesRead, "params": map[string]interface{}{"uri": testResourceURI}}
	_, header, status := postWireRequest(t, wire.baseURL+"/mcp", payload, session, "")
	if status != http.StatusUnauthorized || !strings.Contains(header.Get("WWW-Authenticate"), "authorization_uri=") {
		t.Fatalf("challenge status=%d header=%q", status, header.Get("WWW-Authenticate"))
	}
	// A publication between challenge and exchange must keep the broker's PKCE
	// state while each request uses its own pinned service and policy.
	successorPolicy := testAuthorizationPolicy()
	successorPolicy.Resources[testResourceURI].UseIdToken = true
	successorPolicy.Resources[testResourceURI].ProtectedResourceMetadata.AuthorizationServers = []string{identity.URL + "/next"}
	service = newTransportTestService(successorPolicy)
	source.current.Store(service)
	// A separate HTTP request must find the code verifier created by challenge.
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Mcp-Session-Id", session)
	request.Header.Set(flow.AuthorizationExchangeHeader, (&flow.AuthorizationExchange{Code: "test-code", RedirectURI: "http://localhost/callback"}).ToHeader())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || exchanges.Load() != 1 || service.observedToken() != "Bearer "+idToken {
		t.Fatalf("exchange status=%d count=%d ID-token-forwarded=%t", response.StatusCode, exchanges.Load(), service.observedToken() == "Bearer "+idToken)
	}
	var grant *http.Cookie
	for _, candidate := range response.Cookies() {
		if candidate.Name == "BFF-Auth-Session" {
			grant = candidate
			break
		}
	}
	if grant == nil || grant.Value == "" || !grant.HttpOnly {
		t.Fatal("authorization exchange did not issue an opaque HttpOnly BFF grant")
	}
	postWithGrant := func(sessionID string) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("Mcp-Session-Id", sessionID)
		request.AddCookie(grant)
		result, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Body.Close()
		return result.StatusCode
	}
	if status := postWithGrant(session); status != http.StatusOK {
		t.Fatalf("same-session BFF continuation status=%d", status)
	}
	newSession := initializeWireSession(t, wire.baseURL)
	if status := postWithGrant(newSession); status != http.StatusOK || exchanges.Load() != 1 || service.observedToken() != "Bearer "+idToken {
		t.Fatalf("reconnected BFF continuation status=%d exchanges=%d", status, exchanges.Load())
	}
	metadata, err := http.Get(wire.baseURL + "/.well-known/oauth-protected-resource?resource=" + url.QueryEscape(testResourceURI))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Body.Close()
	if metadata.StatusCode != http.StatusOK {
		t.Fatalf("protected resource metadata status=%d", metadata.StatusCode)
	}
	var published struct {
		AuthorizationServers []string `json:"authorization_servers"`
	}
	if err := json.NewDecoder(metadata.Body).Decode(&published); err != nil || len(published.AuthorizationServers) != 1 || published.AuthorizationServers[0] != identity.URL+"/next" {
		t.Fatalf("protected resource metadata did not follow the pinned successor policy: %v", err)
	}
}

func TestBFFPolicyViewsAreIsolatedConcurrently(t *testing.T) {
	broker := &authBroker{backendForFrontend: &auth.BackendForFrontend{}}
	const count = 32
	var jobs sync.WaitGroup
	for i := 0; i < count; i++ {
		jobs.Add(1)
		go func(index int) {
			defer jobs.Done()
			policy := &authorization.Policy{ExcludeURI: string(rune('a' + index))}
			view, err := broker.view(policy, index%2 == 0)
			if err != nil || view.Config.Policy != policy || view.Config.RequireResourceAuthorization != (index%2 == 0) {
				t.Errorf("policy view %d was not isolated: %v", index, err)
			}
		}(i)
	}
	jobs.Wait()
	if broker.seed == nil || broker.seed.Config.Policy != nil {
		t.Fatal("listener broker seed acquired a request policy")
	}
}

func TestStaticBFFListenerUsesExistingPolicy(t *testing.T) {
	wire := startWireServerConfig(t, Config{
		Service: newTransportTestService(testAuthorizationPolicy()),
		BackendForFrontend: &auth.BackendForFrontend{Client: &oauth2.Config{
			ClientID: "test-client",
			Endpoint: oauth2.Endpoint{AuthURL: "https://idp.example.com/authorize", TokenURL: "https://idp.example.com/token"},
		}, AuthorizationExchangeHeader: flow.AuthorizationExchangeHeader},
		Transport: TransportConfig{Kind: TransportStreamable},
	})
	session := initializeWireSession(t, wire.baseURL)
	payload := map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": schema.MethodResourcesRead, "params": map[string]interface{}{"uri": testResourceURI}}
	_, headers, status := postWireRequest(t, wire.baseURL+"/mcp", payload, session, "")
	if status != http.StatusUnauthorized || !strings.Contains(headers.Get("WWW-Authenticate"), "authorization_uri=") {
		t.Fatalf("static BFF challenge status=%d header=%q", status, headers.Get("WWW-Authenticate"))
	}
}

func TestBFFGrantStoreIsolatedAcrossListeners(t *testing.T) {
	idToken := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":4102444800}`)) + "."
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" || r.FormValue("code") != "test-code" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"test-access","token_type":"Bearer","expires_in":3600,"id_token":"`+idToken+`"}`)
	}))
	defer identity.Close()
	newWire := func() *wireServer {
		t.Helper()
		policy := testAuthorizationPolicy()
		policy.Resources[testResourceURI].UseIdToken = true
		policy.Resources[testResourceURI].ProtectedResourceMetadata.AuthorizationServers = []string{identity.URL}
		return startWireServerConfig(t, Config{
			Service: newTransportTestService(policy),
			BackendForFrontend: &auth.BackendForFrontend{Client: &oauth2.Config{
				ClientID: "test-client", ClientSecret: "test-secret",
				Endpoint: oauth2.Endpoint{AuthURL: identity.URL + "/authorize", TokenURL: identity.URL + "/token", AuthStyle: oauth2.AuthStyleInHeader},
			}, AuthorizationExchangeHeader: flow.AuthorizationExchangeHeader},
			Transport: TransportConfig{Kind: TransportStreamable},
		})
	}
	first := newWire()
	second := newWire() // Construct both transports before the first broker request.
	payload := map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": schema.MethodResourcesRead, "params": map[string]interface{}{"uri": testResourceURI}}
	for _, wire := range []*wireServer{first, second} {
		session := initializeWireSession(t, wire.baseURL)
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("Mcp-Session-Id", session)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("challenge on %s: %d", wire.baseURL, response.StatusCode)
		}
		request, err = http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("Mcp-Session-Id", session)
		request.Header.Set(flow.AuthorizationExchangeHeader, (&flow.AuthorizationExchange{Code: "test-code", RedirectURI: "http://localhost/callback"}).ToHeader())
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("exchange on %s: %d", wire.baseURL, response.StatusCode)
		}
		var grant *http.Cookie
		for _, cookie := range response.Cookies() {
			if cookie.Name == "BFF-Auth-Session" {
				grant = cookie
			}
		}
		if grant == nil {
			t.Fatalf("listener %s issued no BFF grant", wire.baseURL)
		}
		// The first call with the cookie bridges the exchange token from its
		// original MCP session to the stable cookie key.
		request, err = http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("Mcp-Session-Id", session)
		request.AddCookie(grant)
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("cookie bridge on %s: %d", wire.baseURL, response.StatusCode)
		}
		// A cookie-bearing handshake must find this listener's grant in the
		// transport store and rotate it. A grant captured from another listener's
		// process-global default will not rotate here.
		request, err = http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.AddCookie(grant)
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		rotated := false
		for _, cookie := range response.Cookies() {
			rotated = rotated || cookie.Name == grant.Name && cookie.Value != grant.Value
		}
		if response.StatusCode != http.StatusOK || !rotated {
			t.Fatalf("listener %s did not rotate its own BFF grant: status=%d rotated=%t", wire.baseURL, response.StatusCode, rotated)
		}
		newSession := initializeWireSession(t, wire.baseURL)
		request, err = http.NewRequest(http.MethodPost, wire.baseURL+"/mcp", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("Mcp-Session-Id", newSession)
		request.AddCookie(grant)
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("listener %s failed BFF continuation: %d", wire.baseURL, response.StatusCode)
		}
	}
}
