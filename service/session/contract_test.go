package session

import (
	"context"
	"encoding/json"
	"fmt"
	rcontract "github.com/viant/datly/repository/contract"
	_ "github.com/viant/datly/repository/locator/component"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/view/state/kind/locator"
)

type metadataState struct {
	Role        string `parameter:",kind=header,in=X-Role,required=true"`
	Debug       bool   `bind:",kind=query,in=debug"`
	Method      string `parameter:",kind=contract,in=method"`
	Path        string `parameter:",kind=contract,in=path"`
	Initialized bool
}

func (s *metadataState) Init(context.Context) error { s.Initialized = true; return nil }

func TestContractBinderNativeState(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/schema?debug=true&method=DELETE", nil)
	req.Header.Set("X-Role", "reader")
	opts := []locator.Option{locator.WithRequest(req)}
	s := &Session{cache: newCache(), Options: Options{kindLocator: locator.NewKindsLocator(nil, opts...), locatorOptions: opts}}
	binder := s.ContractBinder("PATCH", "/campaigns")
	var got metadataState
	if err := binder.Bind(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Role != "reader" || !got.Debug || got.Method != "PATCH" || got.Path != "/campaigns" || got.Initialized {
		t.Fatalf("state: %+v", got)
	}
	// Same Go field name with a different source must not reuse the first binding.
	var other struct {
		Role string `parameter:",kind=query,in=debug"`
	}
	if err := binder.Bind(context.Background(), &other); err != nil {
		t.Fatal(err)
	}
	if other.Role != "true" {
		t.Fatalf("cross-state cache collision: %+v", other)
	}
	if len(s.cache.values) != 0 {
		t.Fatal("metadata polluted business cache")
	}
}

func TestContractBinderRejectsInvalidState(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/schema", nil)
	opts := []locator.Option{locator.WithRequest(req)}
	s := &Session{cache: newCache(), Options: Options{kindLocator: locator.NewKindsLocator(nil, opts...), locatorOptions: opts}}
	binder := s.ContractBinder("POST", "/campaigns")
	for _, target := range []any{nil, metadataState{}, (*metadataState)(nil), new(int)} {
		if err := binder.Bind(context.Background(), target); err == nil {
			t.Fatalf("accepted %T", target)
		}
	}
	if err := binder.Bind(context.Background(), &metadataState{}); err == nil {
		t.Fatal("required header accepted")
	}
	if err := binder.Bind(context.Background(), &metadataState{}, bindly.WithPlan(nil)); err == nil {
		t.Fatal("ignored unsupported option")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := binder.Bind(ctx, &metadataState{}); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
}

type contractDispatcher struct {
	calls int
	path  *rcontract.Path
}

func (d *contractDispatcher) Dispatch(_ context.Context, path *rcontract.Path, options ...rcontract.Option) (interface{}, error) {
	d.calls++
	d.path = path
	request := rcontract.NewOptions(options...).Request
	if request == nil || request.Header.Get("Authorization") != "Bearer verified" {
		return nil, fmt.Errorf("native request authority missing")
	}
	return &contractAuth{Allowed: true}, nil
}

type contractAuth struct{ Allowed bool }

func TestContractBinderDelegatesNativeComponent(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/schema", nil)
	req.Header.Set("Authorization", "Bearer verified")
	dispatcher := &contractDispatcher{}
	opts := []locator.Option{locator.WithRequest(req), locator.WithDispatcher(dispatcher)}
	s := &Session{cache: newCache(), Options: Options{kindLocator: locator.NewKindsLocator(nil, opts...), locatorOptions: opts}}
	var got struct {
		Auth *contractAuth `parameter:",kind=component,in=GET:/auth/context"`
	}
	binder := s.ContractBinder("PATCH", "/campaigns")
	if err := binder.Bind(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 1 || dispatcher.path.Method != "GET" || dispatcher.path.URI != "/auth/context" || got.Auth == nil || !got.Auth.Allowed {
		t.Fatalf("native dispatch: %+v %+v", dispatcher, got)
	}
	got.Auth = nil
	if err := binder.Bind(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 1 || got.Auth == nil {
		t.Fatal("recomputed auth within one request")
	}
	if err := s.ContractBinder("PATCH", "/campaigns").Bind(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 2 {
		t.Fatal("authorization crossed request binder scope")
	}

}

func TestContractBinderEnumerationQuery(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/schema?mode=active", nil)
	opts := []locator.Option{locator.WithRequest(req)}
	s := &Session{cache: newCache(), Options: Options{kindLocator: locator.NewKindsLocator(nil, opts...), locatorOptions: opts}}
	var got struct {
		Mode int `parameter:",kind=query,in=mode" choice:"0:draft" choice:"10:active" enumAccept:"literal"`
	}
	if err := s.ContractBinder("POST", "/campaigns").Bind(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Mode != 10 {
		t.Fatalf("%+v", got)
	}
}

func TestContractRootBodyEnumKeepsConfiguredDecoderDestination(t *testing.T) {
	request := httptest.NewRequest("POST", "http://localhost/", strings.NewReader(`"active"`))
	var decoded string
	options := []locator.Option{locator.WithRequest(request), locator.WithBodyType(reflect.TypeFor[int]()), locator.WithUnmarshal(func(data []byte, dest interface{}) error {
		if _, ok := dest.(*int); !ok {
			return fmt.Errorf("changed decoder destination %T", dest)
		}
		decoded = string(data)
		return json.Unmarshal(data, dest)
	})}
	scope := &Session{cache: newCache(), Options: Options{kindLocator: locator.NewKindsLocator(nil, options...), locatorOptions: options}}
	var state struct {
		Body int `parameter:",kind=body" choice:"0:draft" choice:"10:active" enumAccept:"literal"`
	}
	if err := scope.ContractBinder("POST", "/").Bind(context.Background(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Body != 10 || decoded != "10" {
		t.Fatalf("body=%d decoder bytes=%s", state.Body, decoded)
	}
}
