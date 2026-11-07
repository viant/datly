package selectorcompanion

import (
	"context"
	"errors"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/bindly/state"
)

func pageTestBinding() bindly.BindingSpec {
	return bindly.BindingSpec{Name: "Page", Path: "Page", Location: state.Location{Kind: "query", In: "page"}, ErrorCode: 400}
}

// These cases are copied from the separately executed pinned69de native selector
// oracle. This lower-level test establishes raw conversion only, not reader
// Fields ordering, HTTP ownership or canonical generation.
func TestDeferredPageMatchesPinnedSingleAndRepeatedValues(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		wantError   bool
		page        int
	}{
		{"absent", "", false, 0},
		{"custom valid", "?page=2", false, 2},
		{"custom malformed fallback absent", "?page=bad", false, 0},
		{"fallback malformed", "?_page=bad", true, 0},
		{"custom malformed fallback valid", "?page=bad&_page=3", false, 3},
		{"custom valid fallback malformed", "?page=2&_page=bad", false, 2},
		{"repeated custom first valid", "?page=2&page=bad&_page=7", true, 0},
		{"repeated custom first malformed", "?page=bad&page=2&_page=7", true, 0},
		{"repeated fallback first valid", "?_page=3&_page=bad", true, 0},
		{"repeated fallback first malformed", "?_page=bad&_page=3", true, 0},
		{"custom zero", "?page=0&_page=7", false, 1},
		{"custom negative", "?page=-2&_page=7", false, 1},
		{"fallback zero", "?_page=0", false, 1},
		{"fallback negative", "?_page=-2", false, 1},
		{"empty custom fallback valid", "?page=&_page=7", false, 1},
		{"literal comma custom fallback valid", "?page=2,3&_page=7", false, 7},
		{"repeated custom both valid", "?page=2&page=3&_page=7", true, 0},
		{"repeated fallback both valid", "?_page=2&_page=3", true, 0},
		{"empty fallback", "?_page=", false, 1},
		{"custom whitespace fallback valid", "?page=%20&_page=7", false, 7},
		{"custom overflow fallback valid", "?page=999999999999999999999999999999&_page=7", false, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := requestprovider.New(httptest.NewRequest("GET", "/rows"+tc.query, nil))
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			source, err := newPageSource(scope.Query(), pageTestBinding())
			if err != nil {
				t.Fatal(err)
			}
			page, err := source.resolve(context.Background())
			if (err != nil) != tc.wantError || page != tc.page {
				t.Fatalf("page=%d error=%v; want page=%d error=%v", page, err, tc.page, tc.wantError)
			}
			if tc.wantError {
				var binding *bindly.BindingError
				var cause *input.Error
				if !errors.As(err, &binding) || !errors.As(err, &cause) || binding.Name != "Page" || binding.Path != "Page" || binding.Kind != "query" || binding.In != "page" || binding.Code != 400 || cause.Cause == nil {
					t.Fatalf("canonical late error ownership lost: %#v", err)
				}
			}
		})
	}
}

type wrappedPageProvider struct{ locator.Provider }

func TestDeferredPageRejectsNonNativeProvider(t *testing.T) {
	native := requestprovider.NewValues().Query()
	composed, err := locator.ComposeProviders("query", locator.ProviderLayer{Name: "native", Provider: native})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []locator.Provider{nil, wrappedPageProvider{native}, composed} {
		if _, err := newPageSource(p, pageTestBinding()); err == nil {
			t.Fatalf("non-native provider accepted: %T", p)
		}
	}
	if _, err := newPageSource(requestprovider.NewValues().Header(), pageTestBinding()); err == nil {
		t.Fatal("foreign native kind accepted")
	}
	binding := pageTestBinding()
	binding.Location.In = ""
	if _, err := newPageSource(native, binding); err == nil {
		t.Fatal("missing canonical query location accepted")
	}
}

func TestDeferredPageUsesResolutionTimeInvocationPolicy(t *testing.T) {
	scope := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"page": {""}, "_page": {"7"}}), requestprovider.WithIgnoreEmptyQueryParameters(true))
	source, err := newPageSource(scope.Query(), pageTestBinding())
	if err != nil {
		t.Fatal(err)
	}
	// Admission checks canonical explicit-false policy separately. This probe
	// establishes that the retained provider is evaluated only with the later
	// invocation context, rather than snapshotted at carrier construction.
	ctx := requestprovider.WithQueryPolicy(context.Background(), requestprovider.QueryPolicy{IgnoreEmptyParameters: false})
	page, err := source.resolve(ctx)
	if err != nil || page != 1 {
		t.Fatalf("premature query-policy snapshot: page=%d err=%v", page, err)
	}
}

func TestDeferredPageUsesCanonicalCustomLocation(t *testing.T) {
	scope := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"customPage": {"3"}, "page": {"bad"}, "_page": {"bad"}}))
	binding := pageTestBinding()
	binding.Location.In = "customPage"
	source, err := newPageSource(scope.Query(), binding)
	if err != nil {
		t.Fatal(err)
	}
	page, err := source.resolve(context.Background())
	if err != nil || page != 3 {
		t.Fatalf("page=%d err=%v", page, err)
	}
}

func TestDeferredPageCustomFallbackNameRetainsOriginalDuplicateLookupSemantics(t *testing.T) {
	for _, tc := range []struct {
		value   string
		page    int
		failure bool
	}{{"3", 3, false}, {"", 1, false}, {"bad", 0, true}} {
		binding := pageTestBinding()
		binding.Location.In = "_page"
		source, err := newPageSource(requestprovider.NewValues(requestprovider.WithQuery(url.Values{"_page": {tc.value}})).Query(), binding)
		if err != nil {
			t.Fatal(err)
		}
		got, err := source.resolve(t.Context())
		if got != tc.page || (err != nil) != tc.failure {
			t.Fatalf("custom _page=%q page=%d err=%v", tc.value, got, err)
		}
	}
}
func TestDeferredPageFallbackKeepsAuthoredErrorMetadata(t *testing.T) {
	binding := pageTestBinding()
	binding.ErrorCode = 422
	binding.ErrorMessage = "page conversion: ${error}"
	source, err := newPageSource(requestprovider.NewValues(requestprovider.WithQuery(url.Values{"_page": {"bad"}})).Query(), binding)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.resolve(t.Context())
	var failure *bindly.BindingError
	if !errors.As(err, &failure) || failure.Code != 422 || failure.Message != binding.ErrorMessage || failure.In != "page" || failure.Name != "Page" || failure.Cause == nil {
		t.Fatalf("authored binding metadata lost: %v", err)
	}
}

// This matrix checks conversion against the pinned legacy session oracle; the
// reader tests separately check scope, projection ordering and executed windows.
func TestRootNumericAliasConversionMatchesLegacySession(t *testing.T) {
	for _, property := range []spec.SelectorProperty{spec.SelectorPropertyLimit, spec.SelectorPropertyOffset} {
		for _, tc := range []struct {
			name, query string
			want        int
			invalid     bool
		}{
			{"absent", "", 0, false},
			{"empty remains zero", "q=&ALIAS=3", 0, false},
			{"negative remains negative", "q=-2&ALIAS=3", -2, false},
			{"valid primary wins", "q=2&ALIAS=3", 2, false},
			{"malformed primary absent alias", "q=abc", 0, false},
			{"malformed primary valid alias", "q=abc&ALIAS=3", 3, false},
			{"overflow primary valid alias", "q=999999999999999999999999&ALIAS=3", 3, false},
			{"malformed alias", "q=abc&ALIAS=abc", 0, true},
			{"empty alias", "q=abc&ALIAS=", 0, false},
			{"repeated primary", "q=2&q=3&ALIAS=4", 0, true},
			{"repeated alias", "ALIAS=2&ALIAS=3", 0, true},
		} {
			t.Run(string(property)+"/"+tc.name, func(t *testing.T) {
				query := strings.ReplaceAll(tc.query, "ALIAS", "_"+string(property))
				scope, err := requestprovider.New(httptest.NewRequest("GET", "/rows?"+query, nil))
				if err != nil {
					t.Fatal(err)
				}
				defer scope.Close()
				binding := pageTestBinding()
				binding.Name = string(property)
				binding.Path = string(property)
				binding.Location.In = "q"
				binding.Extension = &spec.Parameter{QuerySelector: &spec.QuerySelectorBinding{Property: property, View: "rows"}}
				source, err := newPageSource(scope.Query(), binding)
				if err != nil {
					t.Fatal(err)
				}
				got, err := source.resolve(t.Context())
				if got != tc.want || (err != nil) != tc.invalid {
					t.Fatalf("got %d, %v; want %d, invalid=%v", got, err, tc.want, tc.invalid)
				}
				if err != nil {
					var owned *bindly.BindingError
					if !errors.As(err, &owned) || owned.Name != string(property) || owned.In != "q" {
						t.Fatalf("lost authored binding: %v", err)
					}
				}
			})
		}
	}
}

// Child selector conversion follows the pinned legacy Session.SetViewState
// oracle: only the declared primary is consulted, unlike root alias fallback.
func TestDeferredChildNumericPrimaryOnly(t *testing.T) {
	for _, property := range []spec.SelectorProperty{spec.SelectorPropertyLimit, spec.SelectorPropertyOffset} {
		for _, tc := range []struct {
			name, query string
			want        int
			fail        bool
		}{
			{name: "absent"}, {name: "empty", query: "n="}, {name: "valid", query: "n=2", want: 2},
			{name: "root alias absent from child", query: "_limit=3&_offset=4"},
			{name: "malformed cannot fallback", query: "n=bad&_limit=3&_offset=4", fail: true},
			{name: "repeated equal", query: "n=2&n=2", fail: true},
			{name: "repeated empty", query: "n=&n=2", fail: true},
			{name: "repeated malformed", query: "n=2&n=bad", fail: true},
			{name: "overflow", query: "n=999999999999999999999999", fail: true},
		} {
			t.Run(string(property)+"/"+tc.name, func(t *testing.T) {
				scope, err := requestprovider.New(httptest.NewRequest("GET", "/child?"+tc.query, nil), requestprovider.WithIgnoreEmptyQueryParameters(false))
				if err != nil {
					t.Fatal(err)
				}
				defer scope.Close()
				binding := pageTestBinding()
				binding.Name = "Child" + string(property)
				binding.Path = binding.Name
				binding.Location.In = "n"
				binding.Extension = &spec.Parameter{QuerySelector: &spec.QuerySelectorBinding{View: "children", Property: property}}
				source, err := newPageSource(scope.Query(), binding)
				if err != nil {
					t.Fatal(err)
				}
				source.primaryOnly = true
				got, err := source.resolve(t.Context())
				if (err != nil) != tc.fail || got != tc.want {
					t.Fatalf("value=%d err=%v", got, err)
				}
				if tc.fail {
					var failure *bindly.BindingError
					if !errors.As(err, &failure) || failure.Name != binding.Name || failure.Path != binding.Path || failure.In != "n" {
						t.Fatalf("child error identity lost: %v", err)
					}
				}
			})
		}
	}
}
