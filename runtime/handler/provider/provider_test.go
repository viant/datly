package provider

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/structology"
	xhandler "github.com/viant/xdatly/handler"
)

func TestStaticProvider(t *testing.T) {
	candidate := Static(xhandler.ValueKey("tenant"), 42)
	cacheable, ok := candidate.(*provider)
	if !ok {
		t.Fatalf("provider type = %T, want private provider", candidate)
	}
	if candidate.Kind() != "tenant" || candidate.Priority() != 0 || !cacheable.DefaultCacheable() {
		t.Fatalf("provider metadata = (%q, %d, %v)", candidate.Kind(), candidate.Priority(), cacheable.DefaultCacheable())
	}
	locator := candidate.Locate(nil)
	actual, found, err := locator.Value(context.Background(), reflect.TypeOf(0), "")
	if err != nil || !found || actual != 42 {
		t.Fatalf("Value() = (%v, %v, %v), want (42, true, nil)", actual, found, err)
	}
	if _, found, err = locator.Value(context.Background(), reflect.TypeOf(0), "named"); err != nil || found {
		t.Fatalf("named Value() = (_, %v, %v), want (_, false, nil)", found, err)
	}
}

func TestStaticProviderTreatsTypedNilAsUnavailable(t *testing.T) {
	var value *int
	actual, found, err := Static(xhandler.ValueKey("optional"), value).Locate(nil).Value(context.Background(), nil, "")
	if err != nil || found || actual != value {
		t.Fatalf("Value() = (%v, %v, %v), want (typed nil, false, nil)", actual, found, err)
	}
}

func TestConstantsProviderResolvesNamedImmutableValues(t *testing.T) {
	source := map[string]string{"Vendor": "VENDOR"}
	provider, err := Constants(source)
	if err != nil {
		t.Fatalf("Constants() error = %v", err)
	}
	source["Vendor"] = "changed"
	if provider == nil || provider.Kind() != "const" {
		t.Fatalf("provider = %#v", provider)
	}
	actual, found, err := provider.Locate(nil).Value(context.Background(), reflect.TypeOf(""), "vendor")
	if err != nil || !found || actual != "VENDOR" {
		t.Fatalf("Value() = (%v, %v, %v)", actual, found, err)
	}
	if _, found, err = provider.Locate(nil).Value(context.Background(), reflect.TypeOf(""), "Missing"); err != nil || found {
		t.Fatalf("missing Value() = (_, %v, %v)", found, err)
	}
	empty, err := Constants(nil)
	if err != nil || empty != nil {
		t.Fatal("empty constants must not create a provider")
	}
	if _, err = Constants(map[string]string{"Vendor": "one", "vendor": "two"}); err == nil {
		t.Fatal("case-ambiguous constants must fail")
	}
}

func TestComposeConstantsProtectsCanonicalNamesAndFallsThrough(t *testing.T) {
	registered, err := Constants(map[string]string{"Region": "registered", "Tier": "registered"})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := Constants(map[string]string{"Vendor": "canonical"})
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := Constants(map[string]string{"Vendor": "override", "Region": "invocation"})
	if err != nil {
		t.Fatal(err)
	}
	actual := ComposeConstants(canonical, []locator.Provider{registered, invocation}).Locate(nil)
	for name, want := range map[string]string{"Vendor": "canonical", "Region": "invocation", "Tier": "registered"} {
		value, found, lookupErr := actual.Value(context.Background(), reflect.TypeOf(""), name)
		if lookupErr != nil || !found || value != want {
			t.Fatalf("Value(%q) = (%v, %v, %v), want (%q, true, nil)", name, value, found, lookupErr, want)
		}
	}
}

type constantMetadataProvider struct {
	locator.Provider
	priority    int
	cacheable   bool
	unavailable bool
}

func (p *constantMetadataProvider) Priority() int { return p.priority }

func (p *constantMetadataProvider) DefaultCacheable() bool { return p.cacheable }

func (p *constantMetadataProvider) Locate(state *structology.State) locator.Locator {
	if p.unavailable {
		return nil
	}
	return p.Provider.Locate(state)
}

func TestComposeConstantsUsesAuthorityMetadataAndFailsAtUnavailableFallback(t *testing.T) {
	canonical, err := Constants(map[string]string{"Vendor": "canonical"})
	if err != nil {
		t.Fatal(err)
	}
	dynamic, err := Constants(map[string]string{"Region": "dynamic"})
	if err != nil {
		t.Fatal(err)
	}
	fallback := &constantMetadataProvider{Provider: dynamic, priority: 100, cacheable: false, unavailable: true}
	composed := ComposeConstants(canonical, []locator.Provider{fallback})
	policy, ok := composed.(locator.CachePolicy)
	if composed.Priority() != canonical.Priority() || !ok || !policy.DefaultCacheable() {
		t.Fatalf("composed metadata = (priority:%d cacheable:%v)", composed.Priority(), ok && policy.DefaultCacheable())
	}
	actual := composed.Locate(nil)
	value, found, lookupErr := actual.Value(context.Background(), reflect.TypeOf(""), "Vendor")
	if lookupErr != nil || !found || value != "canonical" {
		t.Fatalf("canonical Value() = (%v, %v, %v)", value, found, lookupErr)
	}
	if _, found, lookupErr = actual.Value(context.Background(), reflect.TypeOf(""), "Region"); lookupErr == nil || found {
		t.Fatalf("dynamic Value() = (_, %v, %v), want unavailable-provider error", found, lookupErr)
	}
}

func TestProviderPropagatesResolverResult(t *testing.T) {
	expected := errors.New("lookup failed")
	provider := New(xhandler.ValueKey("dynamic"), func(context.Context) (any, bool, error) {
		return "partial", true, expected
	})
	actual, found, err := provider.Locate(nil).Value(context.Background(), nil, "")
	if actual != "partial" || !found || !errors.Is(err, expected) {
		t.Fatalf("Value() = (%v, %v, %v), want (partial, true, expected)", actual, found, err)
	}
}

func TestInput(t *testing.T) {
	type address struct{ City string }
	type inputType struct {
		ID      int
		Address address
	}
	input := &inputType{ID: 7, Address: address{City: "Warsaw"}}
	state := structology.NewStateType(reflect.TypeOf(input)).WithValue(input)
	actual, found, err := Input().Locate(state).Value(context.Background(), nil, "")
	if err != nil || !found || actual != input {
		t.Fatalf("Value() = (%v, %v, %v), want input", actual, found, err)
	}
	actual, found, err = Input().Locate(state).Value(context.Background(), reflect.TypeOf(0), "ID")
	if err != nil || !found || actual != 7 {
		t.Fatalf("Value(ID) = (%v, %v, %v), want (7, true, nil)", actual, found, err)
	}
	actual, found, err = Input().Locate(state).Value(context.Background(), reflect.TypeOf(""), "Address.City")
	if err != nil || !found || actual != "Warsaw" {
		t.Fatalf("Value(Address.City) = (%v, %v, %v), want (Warsaw, true, nil)", actual, found, err)
	}
	if _, found, err = Input().Locate(state).Value(context.Background(), nil, "Unknown"); err != nil || found {
		t.Fatalf("Value(Unknown) = (_, %v, %v), want (_, false, nil)", found, err)
	}
}

func TestParameterOrdersBetweenTransportAndViewProviders(t *testing.T) {
	provider := Parameter()
	if provider.Kind() != "param" || provider.Priority() != locator.PriorityTransform ||
		!(locator.PrioritySource < provider.Priority() && provider.Priority() < locator.PriorityDependent) {
		t.Fatalf("Parameter() = kind:%q priority:%d", provider.Kind(), provider.Priority())
	}
}

type validationStub struct{}

func (*validationStub) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	return nil, nil
}

func TestCapabilitiesUseCanonicalKeys(t *testing.T) {
	validator := &validationStub{}
	providers := Capabilities(rhandler.InvocationCapabilities{Validator: validator})
	if len(providers) != 1 {
		t.Fatalf("Capabilities() count = %d, want 1", len(providers))
	}
	if providers[0].Kind() != string(rhandler.ValidatorCapabilityKey) {
		t.Fatalf("provider Kind() = %q, want %q", providers[0].Kind(), rhandler.ValidatorCapabilityKey)
	}
	actual, found, err := providers[0].Locate(nil).Value(context.Background(), nil, "")
	if err != nil || !found || actual != validator {
		t.Fatalf("validator Value() = (%v, %v, %v), want validator", actual, found, err)
	}
	if providers := Capabilities(rhandler.InvocationCapabilities{}); len(providers) != 0 {
		t.Fatalf("empty Capabilities() count = %d, want 0", len(providers))
	}
}
