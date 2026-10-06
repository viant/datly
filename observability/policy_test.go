package observability

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/spec"
)

func policyFixture(t *testing.T) (*Policy, spec.Key, *spec.View) {
	t.Helper()
	key := spec.Key{Kind: spec.KindComponent, Scope: "module.one", Name: "records"}
	view := &spec.View{Name: "datafees", Namespace: "adf"}
	id, err := view.Identity()
	if err != nil {
		t.Fatal(err)
	}
	return &Policy{Views: []ViewObservation{{Component: key, ViewIdentity: id, Diagnostic: "datafees#", Operation: &OperationDescriptor{Name: "platform.delivery.advertiser.datafees", Location: "platform/delivery/advertiser", Description: "datafees performance", Provider: Source11}}}}, key, view
}

func TestObservationPolicyFrozenAndDiagnosticOnly(t *testing.T) {
	p, key, view := policyFixture(t)
	r := NewRecorder(nil, WithPolicy(p))
	p.Views[0].Diagnostic = "mutated"
	p.Views[0].Operation.Name = "mutated"
	resolved, err := r.Resolve(key, view)
	if err != nil || resolved.Diagnostic != "datafees#" || resolved.Operation != "platform.delivery.advertiser.datafees" {
		t.Fatalf("frozen policy %+v %v", resolved, err)
	}
	if len(r.operations) != 0 {
		t.Fatal("resolution registered a ghost operation")
	}
	p, key, view = policyFixture(t)
	p.Views[0].Operation = nil
	r = NewRecorder(nil, WithPolicy(p))
	resolved, err = r.Resolve(key, view)
	if err != nil || resolved.Operation != key.String()+"/datafees" || resolved.Diagnostic != "datafees#" {
		t.Fatalf("diagnostic-only owner %+v %v", resolved, err)
	}
	r.Pending(resolved.Operation, 1)
	r.Pending(resolved.Operation, -1)
	if len(r.Values(resolved.Operation)) != 14 {
		t.Fatal("diagnostic override changed provider")
	}
}

func TestObservationPolicyValidationAndFallbackCollisions(t *testing.T) {
	p, key, view := policyFixture(t)
	duplicate := p.Copy()
	duplicate.Views = append(duplicate.Views, duplicate.Views[0])
	if duplicate.Validate() == nil {
		t.Fatal("duplicate target accepted")
	}
	conflict := p.Copy()
	second := conflict.Views[0]
	second.Component.Scope = "module.two"
	op := *second.Operation
	op.Description = "different"
	second.Operation = &op
	conflict.Views = append(conflict.Views, second)
	if conflict.Validate() == nil {
		t.Fatal("conflicting source owner accepted")
	}
	r := NewRecorder(nil, WithPolicy(p))
	if r.ValidateComponents(nil) == nil {
		t.Fatal("unknown component accepted")
	}
	if err := r.ValidateComponents([]spec.Key{key}); err != nil {
		t.Fatal(err)
	}
	if r.ValidateTargets([]ViewTarget{{key, &spec.View{Name: "datafees", Namespace: "other"}}}) == nil {
		t.Fatal("wrong namespace target accepted")
	}
	if r.ValidateTargets([]ViewTarget{{key, view}, {key, &spec.View{Name: "datafees", Namespace: "adf"}}}) == nil {
		t.Fatal("ambiguous effective target accepted")
	}
	if err := r.ValidateTargets([]ViewTarget{{key, view}}); err != nil {
		t.Fatal(err)
	}
	if len(r.operations) != 0 {
		t.Fatal("failed/successful validation registered operations")
	}
	p.Views[0].Operation.Name = spec.Key{Kind: spec.KindComponent, Scope: "module.two", Name: "records"}.String() + "/datafees"
	r = NewRecorder(nil, WithPolicy(p))
	if _, err := r.Resolve(spec.Key{Kind: spec.KindComponent, Scope: "module.two", Name: "records"}, view); err == nil {
		t.Fatal("native fallback merged with source descriptor")
	}
}

func TestObservationDescriptorLocationAndProviderConflicts(t *testing.T) {
	for _, field := range []string{"location", "provider"} {
		t.Run(field, func(t *testing.T) {
			p, _, _ := policyFixture(t)
			second := p.Copy().Views[0]
			second.Component.Scope = "module.other"
			switch field {
			case "location":
				second.Operation.Location = "different/location"
			case "provider":
				second.Operation.Provider = Native14
			}
			p.Views = append(p.Views, second)
			if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "conflicting observation operation") {
				t.Fatalf("%s conflict accepted: %v", field, err)
			}
			r := NewRecorder(nil, WithPolicy(p))
			if r.ValidateComponents([]spec.Key{p.Views[0].Component, p.Views[1].Component}) == nil {
				t.Fatal("invalid descriptor owner became usable")
			}
			if len(r.operations) != 0 {
				t.Fatal("conflicting descriptor registered a ghost counter")
			}
		})
	}
}

func TestObservationSourceProviderCreatedAtCapture(t *testing.T) {
	p, key, view := policyFixture(t)
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	if err != nil {
		t.Fatal(err)
	}
	r.Pending(resolved.Operation, 1)
	r.Begin(resolved.Operation, time.Now())(time.Now(), "Success")
	r.Created(resolved.Operation, "lazy", 1)
	r.Created(resolved.Operation, "warmup", 1)
	r.Pending(resolved.Operation, -1)
	o := r.operations[resolved.Operation]
	if !reflect.DeepEqual(o.Provider.Keys(), viewMetricKeys[:11]) || len(r.Values(resolved.Operation)) != 11 || r.Values(resolved.Operation)["Success"] != 1 || r.Values(resolved.Operation)["Pending"] != 0 {
		t.Fatalf("source owner %+v keys %v values %v", o, o.Provider.Keys(), r.Values(resolved.Operation))
	}
	if o.Location != "platform/delivery/advertiser" || o.Description != "datafees performance" || o.Provider.Map("cache:created") != -1 {
		t.Fatal("source descriptor or publication provider changed")
	}
}

func TestObservationReportingFreshFilteredCatalog(t *testing.T) {
	p, key, view := policyFixture(t)
	second := p.Views[0]
	second.Component.Scope = "module.two"
	descriptor := *second.Operation
	descriptor.Name = "platform.delivery.advertiser.new"
	descriptor.Description = "new performance"
	second.Operation = &descriptor
	p.Views = append(p.Views, second)
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	if err != nil {
		t.Fatal(err)
	}
	r.Begin(resolved.Operation, time.Now())(time.Now(), "Success")
	for _, suffix := range []string{"operations", "operation/" + resolved.Operation, "operation/" + resolved.Operation + "/cumulative/Success", "operation/" + resolved.Operation + "/recent/Success", "operation/" + resolved.Operation + "/recent", "counters", "counter/unknown"} {
		res := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/v1/api/meta/metric/"+suffix, nil)
		r.ServeMetrics("/v1/api/meta/metric/", res, req)
		if res.Code != 200 {
			t.Fatalf("route %s status %d %s", suffix, res.Code, res.Body.String())
		}
		if strings.Contains(res.Body.String(), "cache:created") {
			t.Fatalf("source catalog contains native extension: %s", res.Body.String())
		}
	}
	report := func() string {
		res := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/v1/api/meta/metric/platform/delivery/operations", nil)
		r.ServeMetrics("/v1/api/meta/metric/platform/delivery/", res, req)
		if res.Code != 200 {
			t.Fatal(res.Body.String())
		}
		return res.Body.String()
	}
	if text := report(); !strings.Contains(text, resolved.Operation) {
		t.Fatal(text)
	}
	r.Begin("native/unrelated", time.Now())(time.Now(), "Success")
	// A second source declaration is a separate owner, materialized lazily after
	// the first filtered catalog; no handler retains a stale filtered closure.
	r.Begin("platform.delivery.advertiser.new", time.Now())(time.Now(), "Success")
	if text := report(); !strings.Contains(text, "platform.delivery.advertiser.new") || strings.Contains(text, "native/unrelated") {
		t.Fatal(text)
	}
}
