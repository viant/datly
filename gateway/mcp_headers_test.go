package gateway

import (
	"context"
	"github.com/viant/mcp-protocol/authorization"
	"net/http"
	"strings"
	"testing"
)

func TestContractMCPTransportHeadersRemainRequestScoped(t *testing.T) {
	headers := http.Header{"X-Debug": []string{"true"}, "Authorization": []string{"unverified"}, "Content-Type": []string{"text/plain"}, "Content-Length": []string{"200"}}
	ctx := WithMCPHeaders(context.Background(), headers)
	headers.Set("X-Debug", "false")
	token := &authorization.Token{Token: "verified"}
	ctx = context.WithValue(ctx, authorization.TokenKey, token)
	request, rpcErr := (&Router{}).newToolHTTPRequest(ctx, "PATCH", "http://localhost/campaigns", strings.NewReader(`{}`))
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	(&Router{}).addAuthTokenIfPresent(ctx, request)
	if request.Header.Get("X-Debug") != "true" || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Content-Length") != "" || request.Header.Get("Authorization") != "Bearer verified" {
		t.Fatalf("%+v", request.Header)
	}
	if token.Token != "verified" {
		t.Fatal("mutated authenticated context token")
	}
	isolated, _ := (&Router{}).newToolHTTPRequest(context.Background(), "PATCH", "http://localhost/campaigns", nil)
	if isolated.Header.Get("X-Debug") != "" {
		t.Fatal("headers leaked across requests")
	}
}
