package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	xcontract "github.com/viant/xdatly/contract"
	"gopkg.in/yaml.v3"
)

type docPolicyInput struct {
	*DocPolicyMethods
	Secret string `parameter:",kind=query,in=secret"`
	Reason string `parameter:",kind=query,in=reason"`
}
type DocPolicyMethods struct{}

func (*DocPolicyMethods) LinkedMethod() bool { return true }

type docPolicyOutput struct {
	*DocPolicyMethods
	Public string
	Secret string
}

func (*docPolicyInput) Contract(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	return docPolicy(ctx, b)
}
func (*docPolicyOutput) Contract(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	return docPolicy(ctx, b)
}
func docPolicy(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	var access struct {
		Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
		Debug  bool        `parameter:",kind=header,in=X-Debug"`
	}
	if err := b.Bind(ctx, &access); err != nil {
		return nil, err
	}
	if access.Claims.Subject == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "reader", Field: func(_ string, parentType reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		switch parentType.Name() {
		case "docPolicyInput":
			switch f.Name {
			case "Secret":
				return f.Clone().Hide(), nil
			case "Reason":
				return f.Clone().Required(), nil
			}
		case "docPolicyOutput":
			switch f.Name {
			case "Secret":
				return f.Clone().Hide(), nil
			}
		}
		return nil, nil
	}}, nil
}

func TestOpenAPICallerContractAggregatePathJSONYAMLAndHEAD(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DocPolicy"}, Routes: []*spec.Route{{Method: "GET", Path: "/api/orders"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[docPolicyInput](), OutputType: reflect.TypeFor[docPolicyOutput](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	var seen atomic.Pointer[docPolicyInput]
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, input *docPolicyInput) (*docPolicyOutput, error) {
		calls.Add(1)
		seen.Store(input)
		return &docPolicyOutput{Public: "public", Secret: "server private"}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	h, err := (Config{APIPrefix: "/api", Meta: Meta{OpenApiURI: "/meta", DocURI: " ", CacheWarmURI: " ", CacheInvalidateURI: " "}, OpenAPI: &OpenAPIConfig{Info: openapi3.Info{Title: "Contract", Version: "1"}}}).Build(t.Context(), HandlerInput{Runtime: rt, Components: []*registry.RegisteredComponent{registered}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	baseline, err := h.documents.aggregate.snapshot.Export("json")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/meta", "/meta?format=yaml", "/meta/orders"} {
		for _, subject := range []string{"reader", "admin", "reader"} {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", key.Sign(t, subject, time.Now().Add(time.Hour)))
			req.Header.Set("X-Debug", "true")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("%s %s status=%d body=%s", path, subject, w.Code, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal("personalized document cache policy")
			}
			var document map[string]any
			if strings.Contains(w.Header().Get("Content-Type"), "yaml") {
				err = yaml.Unmarshal(w.Body.Bytes(), &document)
			} else {
				err = json.Unmarshal(w.Body.Bytes(), &document)
			}
			if err != nil {
				t.Fatal(err)
			}
			paths := document["paths"].(map[string]any)
			operation := paths["/api/orders"].(map[string]any)["get"].(map[string]any)
			secret, required := false, false
			for _, item := range operation["parameters"].([]any) {
				param := item.(map[string]any)
				if param["name"] == "secret" {
					secret = true
				}
				if param["name"] == "reason" {
					required, _ = param["required"].(bool)
				}
			}
			if secret != (subject == "admin") || required != (subject != "admin") {
				t.Fatalf("%s secret=%v required=%v", subject, secret, required)
			}
			encoded, _ := json.Marshal(document["components"])
			if strings.Contains(string(encoded), `"Secret"`) != (subject == "admin") {
				t.Fatalf("output visibility %s: %s", subject, encoded)
			}
			head := req.Clone(req.Context())
			head.Method = "HEAD"
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, head)
			if rec.Code != 200 || rec.Body.Len() != 0 {
				t.Fatal("HEAD", rec.Code, rec.Body.String())
			}
		}
	}
	missing := httptest.NewRecorder()
	h.ServeHTTP(missing, httptest.NewRequest("GET", "/meta", nil))
	if missing.Code < 400 {
		t.Fatal("missing credential exposed metadata")
	}
	if calls.Load() != 0 {
		t.Fatal("metadata executed business handler")
	}
	// Concurrent publication must not leak a privileged snapshot into another
	// caller or race on the immutable base document/output schema caches.
	tokens := map[string]string{"admin": key.Sign(t, "admin", time.Now().Add(time.Hour)), "reader": key.Sign(t, "reader", time.Now().Add(time.Hour))}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(admin bool) {
			defer wg.Done()
			subject := "reader"
			if admin {
				subject = "admin"
			}
			req := httptest.NewRequest("GET", "/meta", nil)
			req.Header.Set("Authorization", tokens[subject])
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Errorf("concurrent status %d", w.Code)
				return
			}
			if strings.Contains(w.Body.String(), `"Secret"`) != admin {
				t.Error("concurrent schema crossed caller boundary")
			}
		}(i%2 == 0)
	}
	wg.Wait()
	after, _ := h.documents.aggregate.snapshot.Export("json")
	if string(after) != string(baseline) {
		t.Fatal("base snapshot mutated")
	}
	for _, subject := range []string{"reader", "admin"} {
		req := httptest.NewRequest("GET", "/api/orders?secret=attack&reason=review", nil)
		req.Header.Set("Authorization", tokens[subject])
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("execution %s: %d %s", subject, w.Code, w.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		_, hasSecret := body["Secret"]
		if hasSecret != (subject == "admin") {
			t.Fatalf("execution visibility %s: %s", subject, w.Body.String())
		}
		if subject == "reader" && seen.Load().Secret != "" {
			t.Fatal("hidden query input bound")
		}
		if subject == "admin" && seen.Load().Secret != "attack" {
			t.Fatal("admin input changed")
		}
	}
	before := calls.Load()
	unsupported := httptest.NewRequest("GET", "/api/orders?reason=review&_format=csv", nil)
	unsupported.Header.Set("Authorization", tokens["reader"])
	unsupportedResponse := httptest.NewRecorder()
	h.ServeHTTP(unsupportedResponse, unsupported)
	if unsupportedResponse.Code != 406 || calls.Load() != before {
		t.Fatalf("format admission happened after execution: status=%d calls=%d", unsupportedResponse.Code, calls.Load())
	}
	req := httptest.NewRequest("GET", "/api/orders", nil)
	req.Header.Set("Authorization", tokens["reader"])
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code < 400 || calls.Load() != before {
		t.Fatal("missing required input reached business handler")
	}
}
