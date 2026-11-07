package selectorcompanion

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"github.com/viant/structology"
)

type childQueryProbe struct {
	values  map[string]any
	owned   map[string]bool
	failure error
	calls   int
}

func (p *childQueryProbe) Kind() string                              { return "query" }
func (p *childQueryProbe) Priority() int                             { return 0 }
func (p *childQueryProbe) DefaultCacheable() bool                    { return false }
func (p *childQueryProbe) Locate(*structology.State) locator.Locator { return p }
func (p *childQueryProbe) Owns(name string) bool                     { return p.owned[name] }
func (p *childQueryProbe) Value(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
	p.calls++
	if p.failure != nil {
		return nil, false, p.failure
	}
	v, ok := p.values[name]
	return v, ok, nil
}

func TestComposedQueryRetainsDeferredAuthorityAndPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    map[string]any
		owned     map[string]bool
		fail      bool
		want      int
		wantError bool
	}{
		{name: "inherited", want: 2},
		{name: "child override", values: map[string]any{"page": "4"}, want: 4},
		{name: "authoritative absence", owned: map[string]bool{"page": true, "_page": true}, want: 0},
		{name: "same-name absence then alias", owned: map[string]bool{"page": true}, want: 7},
		{name: "child error", fail: true, wantError: true},
		{name: "repeated override", values: map[string]any{"page": []string{"2", "3"}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"page": {"2"}, "_page": {"7"}})).Query()
			probe := &childQueryProbe{values: tc.values, owned: tc.owned}
			if tc.fail {
				probe.failure = errors.New("child lookup denied")
			}
			composed, err := locator.ComposeProviders("query", locator.ProviderLayer{Name: "child", Provider: probe}, locator.ProviderLayer{Name: "protocol", Provider: native})
			if err != nil {
				t.Fatal(err)
			}
			authority := CarryQueryAuthority(composed, native)
			if authority.(locator.CachePolicy).DefaultCacheable() {
				t.Fatal("composition cache policy lost")
			}
			if authority.Locate(nil).(locator.AuthoritativeLocator).Owns("page") != probe.Owns("page") {
				t.Fatal("locator ownership lost")
			}
			source, err := newPageSource(authority, pageTestBinding())
			if err != nil {
				t.Fatal(err)
			}
			if probe.calls != 0 {
				t.Fatal("lookup occurred at admission")
			}
			got, err := source.resolve(context.Background())
			if got != tc.want || (err != nil) != tc.wantError {
				t.Fatalf("got %d %v want %d error=%v", got, err, tc.want, tc.wantError)
			}
		})
	}
}

func TestComposedQueryNestedAuthorityAndNumericSelectors(t *testing.T) {
	native := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"limit": {"bad"}, "_limit": {"9"}})).Query()
	first, _ := locator.ComposeProviders("query", locator.ProviderLayer{Name: "protocol", Provider: native})
	inherited := CarryQueryAuthority(first, native)
	second, _ := locator.ComposeProviders("query", locator.ProviderLayer{Name: "child", Provider: &childQueryProbe{}}, locator.ProviderLayer{Name: "protocol", Provider: inherited})
	binding := pageTestBinding()
	binding.Location.In = "limit"
	binding.Extension = &spec.Parameter{QuerySelector: &spec.QuerySelectorBinding{Property: spec.SelectorPropertyLimit}}
	source, err := newPageSource(CarryQueryAuthority(second, inherited), binding)
	if err != nil {
		t.Fatal(err)
	}
	got, err := source.resolve(context.Background())
	if err != nil || got != 9 {
		t.Fatalf("nested Limit=%d %v", got, err)
	}
	for _, untrusted := range []locator.Provider{&childQueryProbe{}, wrappedPageProvider{native}, first} {
		if _, err := newPageSource(CarryQueryAuthority(second, untrusted), binding); err == nil {
			t.Fatalf("untrusted inherited source accepted %T", untrusted)
		}
	}
}

func TestComposedQueryRejectsTypedNilComposition(t *testing.T) {
	var absent *childQueryProbe
	native := requestprovider.NewValues().Query()
	got := CarryQueryAuthority(absent, native)
	if _, err := newPageSource(got, pageTestBinding()); err == nil {
		t.Fatal("typed nil composition admitted")
	}
	var absentAuthority *composedQuerySource
	if _, err := newPageSource(absentAuthority, pageTestBinding()); err == nil {
		t.Fatal("typed nil authority admitted")
	}
}
