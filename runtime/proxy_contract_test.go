package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/proxy"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestProxyUsesOrdinaryCustomHandlerBinding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || r.URL.Path != "/target" || r.URL.Query().Get("q") != "value" || string(body) != "body" {
			t.Errorf("wrong forwarded request: method=%s url=%s body=%q", r.Method, r.URL, body)
		}
		if r.Header.Get("X-Gateway-JWT") != "Bearer unchanged" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Errorf("explicit header mapping not respected: %v", r.Header)
		}
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte{0, 255, 1})
	}))
	defer server.Close()
	configuration := fmt.Sprintf(`{"client":{"url":%q,"method":"PUT"},"headers":[{"From":"Authorization","To":"X-Gateway-JWT"}],"forwardQuery":true,"forwardBody":true}`, server.URL+"/target")
	component := componentSpec("Proxy", "POST", "/proxy", []*spec.Parameter{{Name: "Config", Source: spec.BindSource{Kind: "const", Name: "Proxy"}, Value: &configuration}})
	artifact := componentArtifact(t, component, reflect.TypeFor[proxy.Input](), reflect.TypeFor[proxy.Output]())
	// Exactly the public custom-handler adapter used by application contracts.
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[proxy.Output](), Handler: custom.New(proxy.New())}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	req := httptest.NewRequest(http.MethodPost, "http://studio/proxy?q=value&Config=forged&HTTP=forged", strings.NewReader("body"))
	req.Header.Set("Authorization", "Bearer unchanged")
	req.Header.Set("Cookie", "private=1")
	scope, err := request.New(req)
	if err != nil {
		t.Fatal(err)
	}
	result, err := rt.ExecuteRoute(context.Background(), http.MethodPost, "/proxy", scope)
	if err != nil {
		t.Fatal(err)
	}
	output := result.(*proxy.Output)
	body, err := io.ReadAll(output.Body())
	if err != nil || string(body) != string([]byte{0, 255, 1}) || output.StatusCode() != http.StatusAccepted {
		t.Fatalf("proxy changed raw response: %v status=%d err=%v", body, output.StatusCode(), err)
	}
	if len(output.Headers().Values("Set-Cookie")) != 2 {
		t.Fatal("proxy dropped repeated response headers")
	}
}
