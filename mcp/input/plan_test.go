package input

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/structology"
)

type pointerText int

func (p *pointerText) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("value-%d", *p)), nil
}

func TestScopeExposesCanonicalIndependentProviders(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{
		{PublicName: "q", Source: bindstate.Location{Kind: "query", In: "search"}, SourceType: reflect.TypeOf("")},
		{PublicName: "id", Source: bindstate.Location{Kind: "path", In: "userID"}, SourceType: reflect.TypeOf(int(0))},
		{PublicName: "auth", Source: bindstate.Location{Kind: "header", In: "Authorization"}, SourceType: reflect.TypeOf("")},
		{PublicName: "session", Source: bindstate.Location{Kind: "cookie", In: "session"}, SourceType: reflect.TypeOf("")},
		{PublicName: "labels", Source: bindstate.Location{Kind: "form", In: "label"}, SourceType: reflect.TypeOf([]string{})},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(Arguments{
		"q": "active", "id": float64(7), "auth": "Bearer token", "session": "abc", "labels": []interface{}{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	providers := scope.Providers()
	wantKinds := []string{"query", "path", "header", "cookie", "form"}
	if len(providers) != len(wantKinds) {
		t.Fatalf("providers = %+v", providers)
	}
	for index, provider := range providers {
		if provider.Kind() != wantKinds[index] {
			t.Fatalf("provider %d kind = %q", index, provider.Kind())
		}
	}
	assertValue(t, scope.Query(), reflect.TypeOf(""), "search", "active")
	assertValue(t, scope.Path(), reflect.TypeOf(int(0)), "userID", "7")
	assertValue(t, scope.Header(), reflect.TypeOf(""), "Authorization", "Bearer token")
	assertValue(t, scope.Cookie(), reflect.TypeOf(""), "session", "abc")
	assertValue(t, scope.Form(), reflect.TypeOf([]string{}), "label", []string{"a", "b"})
}

func TestScopeNamedBodyUsesExactCanonicalName(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{{
		PublicName: "payload", Source: bindstate.Location{Kind: requestprovider.BodyKind, In: "order"}, SourceType: reflect.TypeOf(struct {
			ID int `json:"id"`
		}{}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(Arguments{"payload": map[string]interface{}{"id": float64(9)}})
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := scope.Body().Locate(nil).Value(context.Background(), plan.arguments[0].SourceType, "order")
	if err != nil || !ok || reflect.ValueOf(value).FieldByName("ID").Int() != 9 {
		t.Fatalf("body value = %#v, %v, %v", value, ok, err)
	}
	if _, ok, err = scope.Body().Locate(nil).Value(context.Background(), plan.arguments[0].SourceType, "Order"); err != nil || ok {
		t.Fatalf("case-variant body miss = %v, %v", ok, err)
	}
}

func TestScopeNormalizesArgumentAliasesWithoutMutatingCallerInput(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{{
		PublicName: "Request", Aliases: []string{"diagnose"},
		Source: bindstate.Location{Kind: requestprovider.BodyKind, In: "diagnose"}, SourceType: reflect.TypeOf(struct {
			ID int `json:"id"`
		}{}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	input := Arguments{"diagnose": map[string]interface{}{"id": float64(9)}}
	scope, err := plan.Scope(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := input["Request"]; ok {
		t.Fatalf("caller arguments were mutated: %+v", input)
	}
	value, ok, err := scope.Body().Locate(nil).Value(context.Background(), plan.arguments[0].SourceType, "diagnose")
	if err != nil || !ok || reflect.ValueOf(value).FieldByName("ID").Int() != 9 {
		t.Fatalf("body value = %#v, %v, %v", value, ok, err)
	}
}

func TestScopeWholeBodyDelegatesTypedDecode(t *testing.T) {
	type payload struct {
		ID int `json:"id"`
	}
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "payload", Source: bindstate.Location{Kind: "body"}, SourceType: reflect.TypeOf(payload{})}})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(Arguments{"payload": map[string]interface{}{"id": float64(11)}})
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, scope.Body(), reflect.TypeOf(payload{}), "", payload{ID: 11})
}

func TestCompileAndScopeFailClosed(t *testing.T) {
	stringType := reflect.TypeOf("")
	tests := []struct {
		name      string
		arguments []Argument
		values    map[string]interface{}
		match     string
	}{
		{name: "duplicate public", arguments: []Argument{{PublicName: "id", Source: bindstate.Location{Kind: "query", In: "a"}, SourceType: stringType}, {PublicName: "id", Source: bindstate.Location{Kind: "query", In: "b"}, SourceType: stringType}}, match: "duplicate MCP argument"},
		{name: "duplicate source", arguments: []Argument{{PublicName: "a", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}, {PublicName: "b", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}}, match: "duplicate MCP source"},
		{name: "unsupported", arguments: []Argument{{PublicName: "id", Source: bindstate.Location{Kind: "component", In: "id"}, SourceType: stringType}}, match: "unsupported binding kind"},
		{name: "mixed body", arguments: []Argument{{PublicName: "body", Source: bindstate.Location{Kind: "body"}, SourceType: stringType}, {PublicName: "name", Source: bindstate.Location{Kind: "body", In: "name"}, SourceType: stringType}}, match: "cannot mix whole and named"},
		{name: "unknown value", arguments: []Argument{{PublicName: "id", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}}, values: map[string]interface{}{"other": "x"}, match: "unknown MCP argument"},
		{name: "duplicate alias", arguments: []Argument{{PublicName: "id", Aliases: []string{"value"}, Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}, {PublicName: "other", Aliases: []string{"value"}, Source: bindstate.Location{Kind: "query", In: "other"}, SourceType: stringType}}, match: "duplicate MCP argument"},
		{name: "public collides with alias", arguments: []Argument{{PublicName: "id", Aliases: []string{"other"}, Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}, {PublicName: "other", Source: bindstate.Location{Kind: "query", In: "other"}, SourceType: stringType}}, match: "duplicate MCP argument"},
		{name: "canonical and alias supplied", arguments: []Argument{{PublicName: "id", Aliases: []string{"value"}, Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: stringType}}, values: map[string]interface{}{"id": "1", "value": "1"}, match: "conflicting MCP argument"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := NewCompiler().Compile(test.arguments)
			if err == nil && test.values != nil {
				_, err = plan.Scope(Arguments(test.values))
			}
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestArgumentsRejectUnexpectedSourceType(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{{PublicName: "id", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeOf(int(0))}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.Scope(Arguments{"id": "four"})
	if err == nil || !strings.Contains(err.Error(), "decode MCP argument") {
		t.Fatalf("error = %v", err)
	}
}

func TestScopeHeaderOverlayIsImmutableAndCaseInsensitive(t *testing.T) {
	plan, err := NewCompiler().Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(Arguments(nil))
	if err != nil {
		t.Fatal(err)
	}
	overlay := scope.WithHeader("authorization", "Bearer secret")
	if value, found, lookupErr := scope.Header().Locate(nil).Value(context.Background(), reflect.TypeOf(""), "authorization"); lookupErr != nil || found || value != nil {
		t.Fatalf("header overlay mutated the original scope: value=%v found=%v err=%v", value, found, lookupErr)
	}
	assertValue(t, overlay.Header(), reflect.TypeOf(""), "AUTHORIZATION", "Bearer secret")
}

func TestScopeUsesSameCanonicalRequestProvidersForURI(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{
		{PublicName: "id", Aliases: []string{"identifier"}, Source: bindstate.Location{Kind: "path", In: "id"}, SourceType: reflect.TypeOf(0)},
		{PublicName: "tag", Aliases: []string{"tags"}, Source: bindstate.Location{Kind: "query", In: "tag"}, SourceType: reflect.TypeOf([]int{})},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(URI{Path: map[string]string{"id": "7"}, Query: url.Values{"tag": {"2", "3"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, scope.Path(), reflect.TypeOf(0), "id", "7")
	assertValue(t, scope.Query(), reflect.TypeOf([]int{}), "tag", []string{"2", "3"})
	for _, source := range []Source{
		URI{Query: url.Values{"id": {"1", "2"}}},
		URI{Query: url.Values{"other": {"1"}}},
		URI{Path: map[string]string{"other": "1"}},
		URI{Path: map[string]string{"identifier": "7"}, Query: url.Values{"tags": {"2"}}},
	} {
		if _, err := plan.Scope(source); err == nil {
			t.Fatal("URI scope expected error")
		}
	}
}

func TestArgumentProjectionUsesCanonicalTextRepresentation(t *testing.T) {
	plan, err := NewCompiler().Compile([]Argument{
		{PublicName: "when", Source: bindstate.Location{Kind: "query", In: "when"}, SourceType: reflect.TypeOf(time.Time{})},
		{PublicName: "custom", Source: bindstate.Location{Kind: "query", In: "custom"}, SourceType: reflect.TypeOf(pointerText(0))},
	})
	if err != nil {
		t.Fatal(err)
	}
	const timestamp = "2026-07-17T09:30:00Z"
	scope, err := plan.Scope(Arguments{"when": timestamp, "custom": float64(7)})
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, scope.Query(), reflect.TypeOf(time.Time{}), "when", timestamp)
	assertValue(t, scope.Query(), reflect.TypeOf(pointerText(0)), "custom", "value-7")
}

func TestArgumentProjectionBindsThroughCanonicalBindlyPlan(t *testing.T) {
	type target struct {
		ID   int
		Tags []int
	}
	arguments := []Argument{
		{PublicName: "id", Source: bindstate.Location{Kind: "path", In: "id"}, SourceType: reflect.TypeOf(0)},
		{PublicName: "tags", Source: bindstate.Location{Kind: "query", In: "tag"}, SourceType: reflect.TypeOf([]int{})},
	}
	projection, err := NewCompiler().Compile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := projection.Scope(Arguments{"id": float64(7), "tags": []interface{}{float64(2), float64(3)}})
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(target{}),
		bindly.BindingSpec{Path: "ID", SourceType: reflect.TypeOf(0), Location: bindstate.Location{Kind: "path", In: "id"}},
		bindly.BindingSpec{Path: "Tags", SourceType: reflect.TypeOf([]int{}), Location: bindstate.Location{Kind: "query", In: "tag"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := injector.ForScope(scope.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	actual := &target{}
	if err := invocation.Bind(context.Background(), actual, bindly.WithPlan(plan)); err != nil {
		t.Fatal(err)
	}
	if actual.ID != 7 || !reflect.DeepEqual(actual.Tags, []int{2, 3}) {
		t.Fatalf("bound target = %+v", actual)
	}
}

func TestArgumentProjectionBindsNonURIKindsThroughCanonicalProviders(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	type target struct {
		Mode    int
		Session bool
		Labels  []int
		Payload payload
	}
	arguments := []Argument{
		{PublicName: "mode", Source: bindstate.Location{Kind: "header", In: "X-Mode"}, SourceType: reflect.TypeOf(0)},
		{PublicName: "session", Source: bindstate.Location{Kind: "cookie", In: "session"}, SourceType: reflect.TypeOf(false)},
		{PublicName: "labels", Source: bindstate.Location{Kind: "form", In: "label"}, SourceType: reflect.TypeOf([]int{})},
		{PublicName: "payload", Source: bindstate.Location{Kind: "body", In: "payload"}, SourceType: reflect.TypeOf(payload{})},
	}
	projection, err := NewCompiler().Compile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := projection.Scope(Arguments{
		"mode": float64(3), "session": true, "labels": []interface{}{float64(4), float64(5)}, "payload": map[string]interface{}{"name": "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(target{}),
		bindly.BindingSpec{Path: "Mode", SourceType: reflect.TypeOf(0), Location: bindstate.Location{Kind: "header", In: "X-Mode"}},
		bindly.BindingSpec{Path: "Session", SourceType: reflect.TypeOf(false), Location: bindstate.Location{Kind: "cookie", In: "session"}},
		bindly.BindingSpec{Path: "Labels", SourceType: reflect.TypeOf([]int{}), Location: bindstate.Location{Kind: "form", In: "label"}},
		bindly.BindingSpec{Path: "Payload", SourceType: reflect.TypeOf(payload{}), Location: bindstate.Location{Kind: "body", In: "payload"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := injector.ForScope(scope.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	actual := &target{}
	if err := invocation.Bind(context.Background(), actual, bindly.WithPlan(plan)); err != nil {
		t.Fatal(err)
	}
	if actual.Mode != 3 || !actual.Session || !reflect.DeepEqual(actual.Labels, []int{4, 5}) || actual.Payload.Name != "Ada" {
		t.Fatalf("bound target = %+v", actual)
	}
}

func assertValue(t *testing.T, provider interface {
	Locate(*structology.State) locator.Locator
}, targetType reflect.Type, name string, want interface{}) {
	t.Helper()
	value, ok, err := provider.Locate(nil).Value(context.Background(), targetType, name)
	if err != nil || !ok || !reflect.DeepEqual(value, want) {
		t.Fatalf("value %s = %#v, %v, %v; want %#v", name, value, ok, err, want)
	}
}
